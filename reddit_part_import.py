#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import io
import sys
import os
import traceback
import argparse
import psycopg2
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


def create_insert_tuple(line, live_id):
    obj = json.loads(line)
    if "kind" in obj and obj["kind"] == "LiveUpdate":
        obj = obj["data"]
    try:
        timestamp = int(obj["created_utc"])
        #dt = datetime.utcfromtimestamp(timestamp)
        id_int = int(obj["id"], 36)

        t = (
            id_int,
            obj["author"],
            timestamp,
            line)
        if live_id is not None:
            t += (live_id,)

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


def create_table(cursor, table, parent, min_date, max_date):
    stmt = "create table if not exists %s (CHECK( created_utc >= %d and created_utc < %d)) INHERITS (%s)" % (
        table,
        min_date, max_date,
        parent)

    cursor.execute(stmt)


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


try:
    conn = psycopg2.connect(
        host=args.host,
        database=args.database,
        user=username,
        password=user_password
        )
except Exception, e:
    print "failed to connect as '%s@%s' to database '%s'" % (
        username,
        args.host,
        args.database)
    traceback.print_exc()
    quit()


#
# Begin Database Connection
#
cur = conn.cursor()
psycopg2.extras.register_default_json(loads=lambda x: x)


status_updater = StatusUpdater()
status_updater.total_files = 1

table_dt = datetime(int(mo.group(1)), int(mo.group(2)), 1)
next_table_dt = table_dt + relativedelta(months=1)


tablename = generate_tablename(args.table, table_dt)


if args.filename.endswith(".bz2"):
    print "detected bz2 file..."
    infile = bz2.BZ2File(args.filename, "r", 1024*1024*4)
    file_length = 0
else:
    infile = open(args.filename, "r")

    # get file length
    infile.seek(0, os.SEEK_END)
    file_length = infile.tell()
    infile.seek(0, os.SEEK_SET)


try:

    create_table(cur, tablename, args.table, table_dt, next_table_dt)

    # update status
    status_updater.current_file = 0
    status_updater.total_val = file_length

    # fields
    db_fields = ["id", "author", "created_utc", "data"]
    if args.live_id is not None:
        db_fields.append("live_id")

    arg_list = "(%s)" % (",".join(["%s"] * len(db_fields)))
    db_cols = "(%s)" % (",".join(db_fields))

    # Core loop
    while True:
        lines = [create_insert_tuple(i, args.live_id) for i in islice(infile, 256)]

        # enough is enough
        if lines is None or len(lines) == 0:
            break

        # update parsed status updater
        status_updater.count += len(lines)

        try:
            values = ','.join(
                cur.mogrify(arg_list, x) for x in lines)
            query = "insert into %s %s values " % (
                args.table, db_cols)
            cur.execute(query + values)

            # update status updater
            status_updater.total_added += len(lines)

        except Exception, e:
            traceback.print_exc()
            quit()

        # update status updater
        if file_length > 0:
            status_updater.current_val = os.lseek(
                infile.fileno(), 0, os.SEEK_CUR)
        status_updater.update()


finally:
    # make sure to close the file
    infile.close()

# commit the data
conn.commit()
status_updater.update(force=True)
print "Completed successfully!"

            
