import sys
import io
import sys
import os
import traceback
import argparse
import psycopg2
import getpass
import datetime
import ujson as json
from psycopg2.extras import Json
from itertools import islice

from status_updater import *


parser = argparse.ArgumentParser(description="reddit json importer")
parser.add_argument('--host', default="localhost", help='host')
parser.add_argument('database', help='database name')
parser.add_argument('table', help="table name")
parser.add_argument('filename', help='filename to import')
parser.add_argument('-u', '--user', help="username")
parser.add_argument('-p', '--password', help="password", action="store_true")
parser.add_argument('--live_id', help="id of live thread")


args = parser.parse_args()

# set our username
username = args.user
if args.user is None or len(args.user) == 0:
    username = getpass.getuser()

# set our password
user_password = ""
if args.password:
    user_password = getpass.getpass(
        "Enter password for %s@%s (%s) : " %(
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
    print "failed to connect as '%s@%s' to database '%s'" %(
        username,
        args.host,
        args.database)
    traceback.print_exc()
    quit()


def create_insert_tuple(line, live_id):
    obj = json.loads(line)
    if "kind" in obj and obj["kind"] == "LiveUpdate":
        obj = obj["data"]
    try:
        timestamp = int(obj["created_utc"])
        dt = datetime.utcfromtimestamp(timestamp)

        t = (
            obj["id"],
            obj["author"],
            timestamp,
            dt,
            line)
        if live_id is not None:
            t += (live_id,)

        return t
    except KeyError, e:
        print "key error"
        print json.dumps(obj, indent=4)
        raise e


#
#
#
cur = conn.cursor()
psycopg2.extras.register_default_json(loads=lambda x: x)


status_updater = StatusUpdater()
status_updater.total_files = 1

with open(args.filename, "r") as infile:

    # get file length
    infile.seek(0, os.SEEK_END)
    file_length = infile.tell()
    infile.seek(0, os.SEEK_SET)


    # update status
    status_updater.current_file = 0 
    status_updater.total_val = file_length

    # fields
    db_fields = [ "id", "author", "created_ut", "created_utc", "data" ]
    if args.live_id is not None:
        db_fields.append("live_id")

    arg_list = "(%s)"%(",".join(["%s"] * len(db_fields)))
    db_cols = "(%s)"%(",".join(db_fields))
    

    while True:
        lines = [create_insert_tuple(i, args.live_id) for i in islice(infile, 10)]

        # enough is enough
        if lines is None or len(lines) == 0:
            break

        # update parsed status updater
        status_updater.count += len(lines)

        try:
            values = ','.join(
                cur.mogrify(arg_list, x) for x in lines)
            query = "insert into %s %s values " %(
                args.table, db_cols)
            cur.execute(query + values)

            # update status updater
            status_updater.total_added += len(lines)

        except Exception, e:
            traceback.print_exc()
            quit()

        
        # update status updater
        status_updater.current_val = os.lseek(
            infile.fileno(), 0, os.SEEK_CUR)
        status_updater.update()


# commit the data
conn.commit()



            
