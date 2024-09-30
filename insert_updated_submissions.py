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
from ezconf import ConfigFile

from status_updater import *




def get_utctimestamp(dt):
    return calendar.timegm(dt.utctimetuple())

def create_insert_tuple(line, live_id=None, type="submissions"):
    corrected_line = re.sub(r"(?<!\\)\\u0000", " ", line)
    corrected_line = re.sub("\00", "<<:NULL:>>", corrected_line)
    obj = json.loads(corrected_line)
    #if '\\u000' in corrected_line or '\u0000' in corrected_line:
    #    print "---- str still has null"
    #    print corrected_line
    #    print obj


    try:
        timestamp = int(obj["created_utc"])
        dt = datetime.utcfromtimestamp(timestamp)
        id_int = int(obj["id"], 36)
        subreddit = obj["subreddit"]
        score = obj["score"]
        ups = obj["ups"]
        downs = obj["downs"]
        upvote_ratio = obj["upvote_ratio"]
        num_comments = obj["num_comments"]

        t = (
            id_int,
            dt,
            obj.get("author", None),
            subreddit,
            score,
            ups,
            downs,
            upvote_ratio,
            num_comments,
            corrected_line)

        return t
    except KeyError as e:
        print("key error")
        print(json.dumps(obj, indent=4))
        raise e
    except Exception as e:
        print("Exception {}", e)
        traceback.print_exc()
        raise e












class ConnectionWrapper(object):

    def __init__(self, host, port, database, username, password, isasync=False):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.conn = None
        self.create_connection(isasync=isasync)
        self.isasync = isasync
        self.cursor = None

    def create_connection(self, isasync=False):
        self.close()
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password
                )
            self.isasync = isasync
        except Exception as e:
            print("failed to connect as '%s@%s:%d' to database '%s'" % (
                self.username,
                self.host,
                self.port,
                self.database))
            traceback.print_exc()
            quit()


    def _ensure_cursor(self):
        if self.cursor is None:
            if self.isasync:
                wait_select(self.conn)
            self.cursor = self.conn.cursor()


    def execute(self, query):
        self._ensure_cursor()

        if self.isasync is True:
            wait_select(self.conn)

        self.cursor.execute(query)


    def close_cursor(self):
        if self.cursor is not None:
            if self.cursor.closed is False:
                if self.isasync is True:
                    wait_select(self.conn)

                self.cursor.close()
            self.cursor = None

    def close(self):
        if self.conn is not None:
            self.close_cursor()

            if self.isasync is True:
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
parser.add_argument('--port', type=int, default=5433, help="port")
parser.add_argument('database', help='database name')
parser.add_argument('table', help="table name")
parser.add_argument('filename', help='filename to import')
parser.add_argument('-u', '--user', help="username")
parser.add_argument('-p', '--password', help="password", action="store_true")
parser.add_argument('-s', '--size', type=int, default=64, help="number of record to read at one time")
parser.add_argument('--live_id', help="id of live thread")
parser.add_argument('--config', default='config.json', help="a json config file for storing settings")


args = parser.parse_args()


chunk_size = 64

tablename = args.table


if os.path.isfile(args.config):
    cfg = ConfigFile(args.config)
    if cfg is not None:
        print("using config file")
        db_host = cfg.getValue("database.host", args.host)
        db_port = cfg.getValue("database.port", args.port)
        db_user = cfg.getValue("database.user", args.user)
        db_password = cfg.getValue("database.pass", args.password)
        chunk_size = cfg.getValue("chunk_size", args.size)




conn = ConnectionWrapper(
        host=db_host,
        port=db_port,
        database=args.database,
        username=db_user,
        password=db_password,
        isasync=False
    )


#
# Begin Database Connection
#
conn.execute("SET TIME ZONE 'UTC';")
psycopg2.extras.register_default_json(loads=lambda x: x)


status_updater = StatusUpdater()
status_updater.total_files = 1



inserted_ids = set()

infile = open(args.filename, "r")

# get file length
infile.seek(0, os.SEEK_END)
file_length = infile.tell()
infile.seek(0, os.SEEK_SET)


overflow_lines = []

try:

    # update status
    status_updater.current_file = 0
    status_updater.total_val = file_length

    # fields
    db_fields = [
        "id", "created_utc", "author", "subreddit", "score",
        "ups", "downs", "upvote_ratio", "num_comments", "data"
    ]

    arg_list = "(%s)" % (",".join(["%s"] * len(db_fields)))
    db_cols = "(%s)" % (",".join(db_fields))

    # Core loop
    while True:
        all_lines = [create_insert_tuple(i, args.live_id, args.table) for i in islice(infile, 64)]

        if len(all_lines) == 0:
            break



        #print len(lines), len(overflow_lines)

        for l in all_lines:
            if l[0] in inserted_ids:
                print("duplicate!")
            else:
                inserted_ids.add(l[0])


        try:
            values = []
            query = None
            if len(all_lines) > 0:
                values = b",".join(
                    conn.cursor.mogrify(arg_list, x) for x in all_lines)
                query = "insert into %s %s values " % (
                    tablename, db_cols)

                conn.execute(query + values.decode("utf8"))
                #print "\n" * 4, query
                #print "\n".join([repr(v) for v in lines])
        except Exception as e:
            print("EXCEPTION: {}", e)
            traceback.print_exc()
            print("-" * 78)
            print("total items added:", status_updater.total_added)
            print(query)
            #for i, v in enumerate(all_lines):
            #    print("<>"*30, "\n", str(i), "\n", str(v), "\n\n")
            print()
            quit()


        try:
            if len(overflow_lines) > 0:
                values = b",".join(
                    conn.cursor.mogrify(arg_list, x) for x in overflow_lines)
                query = "insert into %s %s values " % (
                    next_tablename, db_cols)
                conn.execute(query + values.decode("utf8"))


            # update status updater
            status_updater.count += len(all_lines)
            status_updater.total_added += len(all_lines)

        except Exception as e:
            print("EXCEPTION(overflowlines): {}", e)
            traceback.print_exc()
            print("-" * 78)
            print("total items added: {}", status_updater.total_added)
            print(query)
            for i, v in enumerate(overflow_lines):
                print(("<>"*30) + "\n" + i + "\n" + v + "\n\n")
            print(values)
            quit()


        # update status updater
        if file_length > 0:
            status_updater.current_val = os.lseek(
                infile.fileno(), 0, os.SEEK_CUR)
        status_updater.update()


    status_updater.update(force=True)
    # create indexes
    
    
    
    


except Exception as e:
    print("EXCEPTION: {}", e)
    traceback.print_exc()


finally:
    # make sure to close the file
    infile.close()
    #quit()

# commit the data
#conn.commit()
conn.close()
status_updater.update(force=True)
print("Completed successfully!")

            
