#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import io
import sys
import os
import traceback
import argparse
import psycopg2
from psycopg2.extras import wait_select
import getpass
from datetime import datetime
import bz2
import re
import ujson as json
from psycopg2.extras import Json
from itertools import islice
from dateutil.relativedelta import relativedelta
import time
import calendar

from status_updater import *


def create_insert_tuple(line, live_id=None, type="submissions"):
    corrected_line = re.sub(r"(?<!\\)\\u0000", " ", line)
    obj = json.loads(corrected_line)
    if "kind" in obj and obj["kind"] == "LiveUpdate":
        obj = obj["data"]
    try:
        timestamp = int(obj["created_utc"])
        dt = datetime.utcfromtimestamp(timestamp)
        id_int = int(obj["id"], 36)

        t = (
            id_int,
            obj.get("author", None),
            dt,
            line)
        if live_id is not None:
            t += (live_id,)
        elif type.lower() == "comments":
            article = obj["link_id"]
            article = int(article[3:],36) if article.startswith("t3") else article
            parent = obj["parent_id"]
            parent = int(parent[3:], 36) if parent is not None and parent[:2].lower() == 't1' else None
            root_comment = id_int if parent is None else None
            depth = 1 if parent is None else None
            t += (parent, article, depth, root_comment)

        return t
    except KeyError, e:
        print "key error"
        print json.dumps(obj, indent=4)
        raise e
    except Exception, e:
        print "Exception", e
        traceback.print_exc()
        raise e






def generate_tablename(tablebase, table_datetime):
    return "%s_y%04d_m%02d" % (
        tablebase,
        table_datetime.year,
        table_datetime.month)


def get_utctimestamp(dt):
    return calendar.timegm(dt.utctimetuple())


def create_table(conn, table, parent, min_date, max_date):
    stmt = "create table if not exists %s (CHECK( created_utc >= '%s' and created_utc < '%s')) INHERITS (%s)" % (
        table,
        min_date, max_date,
        parent)

    conn.execute(stmt)

def vacuum(conn, tablename):
    #print ">> vacuum"
    conn.create_connection(async=False)
    #print "stroing isolation level"
    old_isolation_level = conn.conn.isolation_level
    #print "setting isolation level"
    conn.conn.set_isolation_level(0)
    query = "VACUUM FULL {}".format(tablename)
    #print "executing query"
    try:
        conn.execute(query)
    except Exception, e:
        print "Exception: ", e
        traceback.print_exc()
        quit()

    #print "reset isolation level"
    conn.conn.set_isolation_level(old_isolation_level)

def verify_date_range(begin, end, dt):
    #val = dt >= begin and dt < end
    #print begin, end, dt, val
    return dt >= begin and dt < end



class ConnectionWrapper(object):

    def __init__(self, host, database, username, password, async=False):
        self.host = host
        self.database = database
        self.username = username
        self.password = password
        self.conn = None
        self.create_connection(async=async)
        self.async = async
        self.cursor = None

    def create_connection(self, async=False):
        self.close()
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.username,
                password=self.password,
                async=async
                )
            self.async = async
        except Exception, e:
            print "failed to connect as '%s@%s' to database '%s'" % (
                username,
                args.host,
                args.database)
            traceback.print_exc()
            quit()


    def _ensure_cursor(self):
        if self.cursor is None:
            if self.async:
                wait_select(self.conn)
            self.cursor = self.conn.cursor()


    def execute(self, query):
        self._ensure_cursor()

        if self.async is True:
            wait_select(self.conn)

        self.cursor.execute(query)


    def close_cursor(self):
        if self.cursor is not None:
            if self.cursor.closed is False:
                if self.async is True:
                    wait_select(self.conn)

                self.cursor.close()
            self.cursor = None

    def close(self):
        if self.conn is not None:
            self.close_cursor()

            if self.async is True:
                wait_select(self.conn)
            else:
                self.conn.commit()

            if self.conn.closed is False:
                self.conn.close()
            self.conn = None

#
# PARSER
#

parser = argparse.ArgumentParser(description="reddit json importer")
parser.add_argument('--host', default="localhost", help='host')
parser.add_argument('database', help='database name')
parser.add_argument('table', help="table name")
parser.add_argument('filename', help='filename to import')
parser.add_argument('-u', '--user', help="username")
parser.add_argument('-p', '--password', help="password", action="store_true")
parser.add_argument('--live_id', help="id of live thread")


args = parser.parse_args()

mo = re.match("R[SC]_(\d{4})-(\d{2}).*", os.path.basename(args.filename), re.I)
if mo is None:
    print "Couldn't parse date from filename. (expects R[SC]_YYYY-MM.*)"
    quit(1)


#print "filename matches: ", mo.group(1), mo.group(2)


# set our username
username = args.user
if args.user is None or len(args.user) == 0:
    username = getpass.getuser()

# set our password
user_password = ""
if args.password:
    user_password = getpass.getpass(
        "Enter password for %s@%s (%s) : " % (
            username,
            args.host,
            args.database))


conn = ConnectionWrapper(
        host=args.host,
        database=args.database,
        username=username,
        password=user_password,
        async=True
    )


#
# Begin Database Connection
#
conn.execute("SET TIME ZONE 'UTC';")
psycopg2.extras.register_default_json(loads=lambda x: x)


status_updater = StatusUpdater()
status_updater.total_files = 1

table_dt = datetime(int(mo.group(1)), int(mo.group(2)), 1)
next_table_dt = table_dt + relativedelta(months=1)
next_next_table_dt = next_table_dt + relativedelta(months=1)


tablename = generate_tablename(args.table, table_dt)
next_tablename = generate_tablename(args.table, next_table_dt)

inserted_ids = set()

if args.filename.endswith(".bz2"):
    print "detected bz2 file..."
    infile = bz2.BZ2File(args.filename, "r", 1024*1024*32)
    file_length = 0
else:
    infile = open(args.filename, "r")

    # get file length
    infile.seek(0, os.SEEK_END)
    file_length = infile.tell()
    infile.seek(0, os.SEEK_SET)


try:
    create_table(conn, tablename, args.table, table_dt, next_table_dt)
    create_table(conn, next_tablename, args.table, next_table_dt, next_next_table_dt)

    # update status
    status_updater.current_file = 0
    status_updater.total_val = file_length

    # fields
    db_fields = ["id", "author", "created_utc", "data"]
    if args.live_id is not None:
        db_fields.append("live_id")
    elif args.table.lower() == "comments":
        db_fields += ["parent_id", "num_children", "depth", "root_comment"]
        #print db_fields

    arg_list = "(%s)" % (",".join(["%s"] * len(db_fields)))
    db_cols = "(%s)" % (",".join(db_fields))

    # Core loop
    while True:
        all_lines = [create_insert_tuple(i, args.live_id, args.table) for i in islice(infile, 512)]

        if len(all_lines) == 0:
            break


        lines = [l for l in all_lines if verify_date_range(table_dt, next_table_dt, l[2])]
        overflow_lines = [l for l in all_lines if verify_date_range(next_table_dt, next_next_table_dt, l[2])]

        #print len(lines), len(overflow_lines)

        for l in lines:
            if l[0] in inserted_ids:
                print "duplicate!"
            else:
                inserted_ids.add(l[0])


        try:
            if len(lines) > 0:
                values = ','.join(
                    conn.cursor.mogrify(arg_list, x) for x in lines)
                query = "insert into %s %s values " % (
                    tablename, db_cols)
                conn.execute(query + values)
                #print "\n" * 4, query
                #print "\n".join([repr(v) for v in lines])


            if len(overflow_lines) > 0:
                values = ','.join(
                    conn.cursor.mogrify(arg_list, x) for x in overflow_lines)
                query = "insert into %s %s values " % (
                    next_tablename, db_cols)
                conn.execute(query + values)


            # update status updater
            status_updater.count += len(all_lines)
            status_updater.total_added += len(all_lines)

        except Exception, e:
            print "EXCEPTION: ", e
            traceback.print_exc()
            quit()

        # update status updater
        if file_length > 0:
            status_updater.current_val = os.lseek(
                infile.fileno(), 0, os.SEEK_CUR)
        status_updater.update()


    status_updater.update(force=True)
    # create indexes
    
    
    
    
        
    try:
        print "\tadding indexes..."
        primary_key_sql = "ALTER TABLE {} ADD PRIMARY KEY (id);".format(tablename)
        conn.execute(primary_key_sql)

        id_brin_sql = "CREATE INDEX {}_id_brin_idx ON {} using BRIN (id);".format(tablename, tablename)
        conn.execute(id_brin_sql)

        date_index_sql = "CREATE INDEX ON {} (created_utc);".format(tablename)
        conn.execute(date_index_sql)

        date_brin_sql = "CREATE INDEX {}_created_utc_brin_idx ON {} (created_utc);".format(tablename, tablename)
        conn.execute(date_brin_sql)

        #wait_select(conn)
        #conn.commit()
        print "\tvacuuming..."
        #conn.close()

        vacuum(conn, tablename)
        pass
    except Exception, e:
        print "EXCEPTION: ", e
        if conn is not None and conn.cursor is not None and conn.query is not None:
            print "QUERY: ", conn.cursor.query
        else:
            print "NULL QUERY"
        traceback.print_exc()
        quit()

except Exception, e:
    print "EXCEPTION:", e
    traceback.print_exc()


finally:
    # make sure to close the file
    infile.close()
    #quit()

# commit the data
#conn.commit()
conn.close()
status_updater.update(force=True)
print "Completed successfully!"

            
