#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-


import argparse
from utils.db_helpers import ConnectionWrapper, get_connection
from utils.iterhelpers import StringIteratorIO
from utils.stopwatch import StopWatchCollection
from itertools import islice
from ezconf import ConfigFile

from io import BytesIO, BufferedRandom, StringIO
import numpy as np
from datetime import datetime, timezone
from typing import Optional, Any, Dict, Iterator, Tuple

import ujson as json
import psycopg2
import re
import os
import io
import stat
import traceback

import bz2
import lzma

from status_updater import *
from dateutil.relativedelta import relativedelta
from struct import pack, Struct





POSTGRES_EPOCH = 946684800

_CREATE_BASE_SQL = "create unlogged table if not exists {} partition of {} for values from ('{}') to ('{}');"

_CREATE_COMMENT_TABLE_SQL = \
"create {} table if not exists {} (LIKE comments including defaults including constraints);"


_CREATE_SUBMISSION_TABLE_SQL = \
"create {} table if not exists {} (LIKE submissions including defaults including constraints);"


def timestamp_to_pgtimestamp(ts):
    return (ts - POSTGRES_EPOCH) * 1000000

def clean_csv_value(value: Optional[Any]) -> str:
    """
    Needs to properly escape these things
    """
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n').replace('\0', '<<:NULL:>>')



def get_article(article: str) -> int:
    """
    returns the article id.

    Not all of the comments have article ids, so we have to check for type
    """
    return int(article[3:],36) if article is not None and article[:2] == 't3' else None



def get_parent(id: str) -> Any:
    """
    get's the parent id
    """
    return int(id[3:], 36) if id is not None and id[:2] == 't1' else None


def get_sub_id(id: str) -> Any:
    """
    Get's hte id of the sub
    """
    sub = int(id[3:], 36) if id is not None and id[:2] == 't5' else None


def fix_unicode_string(line):
    """
    Fixes null escapes appearing from json that gets interpreted by postgres
    """
    s = re.sub(r"(?<!\\)\\u0000", " ", line)
    s = re.sub("\00", "<<:NULL:>>", s)

    return s.replace('"', '""')


def verify_date_range(begin, end, timestamp):
    #val = dt >= begin and dt < end
    #print begin, end, dt, val
    return timestamp >= begin and timestamp < end


def generate_tablename(tablebase, table_datetime):
    return "%s_y%04d_m%02d" % (
        tablebase,
        table_datetime.year,
        table_datetime.month)



def create_table(conn, table, thing_type, min_date, max_date, drop=False, truncate=False, logged=True):
    # remake the table
    if drop is True:
        conn.execute("drop table if exists {};".format(table))

    sql = None
    log_str = "unlogged" if logged is False else ""
    #conn.execute(_CREATE_BASE_SQL.format(table, thing_type, min_date, max_date))
    if thing_type.lower() == "submissions":
        conn.execute(
            _CREATE_SUBMISSION_TABLE_SQL.format(log_str, table))

    elif thing_type.lower() == "comments":
        conn.execute(
            _CREATE_COMMENT_TABLE_SQL.format(log_str, table))

    else:
        raise Exception("Unknown thing type:" + thing_type)

    if truncate is True:
        conn.execute("truncate table {};".format(table))



def vacuum(conn, tablename):
    #print ">> vacuum"
    conn.create_connection(async_conn=False)
    #print "stroing isolation level"
    old_isolation_level = conn.conn.isolation_level
    #print "setting isolation level"
    conn.conn.set_isolation_level(0)
    query = "VACUUM ANALYZE  {}".format(tablename)
    #print "executing query"
    try:
        conn.execute(query)
    except Exception as e:
        print("Exception: ", e)
        traceback.print_exc()
        quit()

    #print "reset isolation level"
    conn.conn.set_isolation_level(old_isolation_level)



def copy_string_iterator_comment(conn, table, rows: Iterator[Tuple[str,Dict[str, Any]]], size: int = 8192) -> None:
    try:
        string_iter = StringIteratorIO((
            '|'.join(
                map(
                    clean_csv_value, 
                    (
                        int(obj["id"], 36),
                        datetime.utcfromtimestamp(int(obj["created_utc"])),
                        get_article(obj.get("link_id",None)),
                        get_article(obj.get("subreddit_id", None)),
                        get_article(obj.get("subreddit_id", None)),
                        obj.get("author", None),
                        obj.get("subreddit", None),
                        '"' + fix_unicode_string(line) + '"'
                        )
                    )
                ) + '\n'
            for (line,obj) in rows

            ))

        #conn.cursor.copy_from(string_iter, table, size=size )
        conn.cursor.copy_expert("copy %s from stdin with csv freeze delimiter '|' null '\\N'" % (table), string_iter, size=size )

    except Exception as e:
        with open(table + ".err", "w+") as f:
                f.write("\n".join([a[0] for a in rows]))
                f.write("\n"*4)


                for line, obj in rows:
                    f.write(
                        '|'.join(
                            [m for m in map(
                                clean_csv_value, 
                                (
                                    int(obj["id"], 36),
                                    datetime.utcfromtimestamp(int(obj["created_utc"])),
                                    get_article(obj.get("link_id",None)),
                                    get_sub_id(obj.get("subreddit_id", None)),
                                    get_parent(obj.get("parent_id", None)),
                                    obj.get("author", None),
                                    obj.get("subreddit", None),
                                    '"' + fix_unicode_string(line) + '"'
                                )
                            )] ))



        print("-----")

        print(e)
        quit(1)




_row_header_struct = Struct("!hiqiq")
_row_int_struct = Struct("!i")

_row_null_val = pack("!i", -1)





class BinaryCopy():
    def __init__(self, chunk_size=256, buffer_size=64*1024*1024):
        self.chunk_size = chunk_size
        self.buffer_size = buffer_size
        self.fs = BufferedRandom(BytesIO(), buffer_size = buffer_size)

        self._row_header_struct = Struct("!hiqiq")
        self._row_int_struct = Struct("!i")
        self._row_bigint_struct = Struct("!iq")
        self._row_null_val = pack("!i", -1)


    def write_binary_header(self):
        self.fs.write(pack('!11sii', b'PGCOPY\n\xff\r\n\0', 0, 0))


    def write_binary_string(self, obj, keyname):
        try:
            val = obj[keyname]

            if val is None:
                self.fs.write(self._row_null_val)
            else:
                val = val.encode()
                self.fs.write(self._row_int_struct.pack(len(val)))
                self.fs.write(val)
        except KeyError as e:
            self.fs.write(self._row_null_val)


    def write_binary_bigint(self, val):
        if val is None:
            self.fs.write(self._row_null_val)
        else:
            self.fs.write(self._row_bigint_struct.pack(8, val))

    def write_comment_row(self, row):
        obj = row[1]
        self.fs.write(_row_header_struct.pack(8,
            8, int(obj["id"], 36), 
            8, timestamp_to_pgtimestamp(int(obj["created_utc"]))
            ))

        # write article id
        # these have to be written separately because of possible null
        self.write_binary_bigint(get_article(obj.get("link_id", None)))
        self.write_binary_bigint(get_sub_id(obj.get("subreddit_id", None)))
        self.write_binary_bigint(get_parent(obj.get("parent_id", None)))




        # write strings
        self.write_binary_string(obj, "author")
        self.write_binary_string(obj, "subreddit")

        # write jsonb data
        data = row[0].encode()
        self.fs.write(pack("!ib", len(data) +1, 1))
        self.fs.write(data)


    def write_submission_row(self, row):
        obj = row[1]
        self.fs.write(_row_header_struct.pack(6,
            8, int(obj["id"], 36), 
            8, timestamp_to_pgtimestamp(int(obj["created_utc"]))
            ))

        # write article id
        # these have to be written separately because of possible null
        self.write_binary_bigint(get_sub_id(obj.get("subreddit_id", None)))

        # write strings
        self.write_binary_string(obj, "author")
        self.write_binary_string(obj, "subreddit")

        # write jsonb data
        data = row[0].encode()
        self.fs.write(pack("!ib", len(data) +1, 1))
        self.fs.write(data)


    def copy_comments(self, conn, table, lines):
        self.write_binary_header()

        for l in lines:
            self.write_comment_row(l)

        # write end of task
        self.fs.write(pack('!h', -1))
        self.fs.flush()
        self.fs.seek(0)

        conn.cursor.copy_expert("copy %s from stdin with binary " % (table), self.fs )

        self.fs.seek(0)
        self.fs.truncate()



def fix_linux_pipe(fd):
    import fcntl
    if not hasattr(fcntl, 'F_SETPIPE_SZ'):
        import platform

        if platform.system() == 'Linux':
            fcntl.F_SETPIPE_SZ = 1031
            print("fixed pipe size")

            fcntl.fcntl(fd, fcntl.F_SETPIPE_SZ, 1024*1024)
    



#
# PARSER
#

parser = argparse.ArgumentParser(description="reddit json importer")
parser.add_argument('--host', default="localhost", help='host')
parser.add_argument('--port', type=int, default=5432, help="port")
parser.add_argument('database', help='database name')
parser.add_argument('filename', help='filename to import')
parser.add_argument('-u', '--user', help="username")
parser.add_argument('-p', '--password', help="password", action="store_true")
parser.add_argument('-s', '--size', type=int, default=512, help="number of record to read at one time")
parser.add_argument('--pseudo', help="a pseudofilename for working with pipes instead of direct intput")
parser.add_argument('--config', default='config.json', help="a json config file for storing settings")


args = parser.parse_args()




db_host = args.host
db_port = args.port
db_user = args.user
db_password = None if args.password is True else ""
chunk_size = args.size
io_buffer_size = 4 * 1024 * 1024
pipe_size = 1024 * 1024


if os.path.isfile(args.config):
    cfg = ConfigFile(args.config)
    if cfg is not None:
        db_host = cfg.getValue("database.host", args.host)
        db_port = cfg.getValue("database.port", args.port)
        db_user = cfg.getValue("database.user", args.user)
        db_password = cfg.getValue("database.pass", args.password)
        chunk_size = cfg.getValue("chunk_size", args.size)
        io_buffer_size = cfg.getValue("io_buffer_size", io_buffer_size)
        pipe_size = cfg.getValue("pipe_size", pipe_size)




# status updater
status_updater = StatusUpdater(update_time=10)
status_updater.total_files = 1


sw = StopWatchCollection()
sw.start("all")


# set up table name data
fn = args.filename
if fn == "-":
    if not args.hasattr("pseudo") or args.pseudo is none:
        print("if using stdin, you must specify a pseudo filename using --pseudo")
        quit(1)
    else:
        fn = args.pseudo



mo = re.match("R([SC])_(\d{4})-(\d{2}).*", os.path.basename(fn), re.I)
if mo is None:
    mo = re.match("R([SC])_(\d{4})-(\d{2}).*", args.pseudo, re.I)
    if mo is None:
        print("Couldn't parse date from filename. (expects R[SC]_YYYY-MM.*)", os.path.basename(fn))
        quit(1)




# set table names
table_base_name = "comments" if mo.group(1).lower() == 'c' else 'submissions'
table_dt = datetime(int(mo.group(2)), int(mo.group(3)), 1, tzinfo=timezone.utc)
next_table_dt = table_dt + relativedelta(months=1)
next_next_table_dt = next_table_dt + relativedelta(months=1)

table_uts = int(table_dt.timestamp())
next_table_uts = int(next_table_dt.timestamp())
next_next_table_uts = int(next_next_table_dt.timestamp())


tablename = generate_tablename(table_base_name, table_dt)
next_tablename = generate_tablename(table_base_name, next_table_dt)


# set default buffer size before opening files
io.DEFAULT_BUFFER_SIZE = io_buffer_size



if args.filename.endswith(".bz2"):
    print("detected bz2 file...")
    #infile = bz2.BZ2File(args.filename, "rt")
    infile = bz2.open(args.filename, "rt", encoding="utf8")
    file_length = 0
elif args.filename.endswith(".xz"):
    print("detected xz file...")
    infile = lzma.open(args.filename, "r", encoding="utf8")
    file_length = 0
else:
    infile = open(args.filename, "r")

    file_length = 0
    if stat.S_ISFIFO(os.stat(args.filename).st_mode) is False:
        # get file length
        try:
            infile.seek(0, os.SEEK_END)
            file_length = infile.tell()
            infile.seek(0, os.SEEK_SET)
        except io.UnsupportedOperation:
            file_length = 0
    else:
        fix_linux_pipe(infile)

# build a connection
sw.start("db connection")
conn = get_connection(db_user,db_password, db_host, db_port, args.database, async_conn=False)
sw.stop("db connection")


try:
    # create table for this month and next
    sw.start("table creation")
    create_table(conn, tablename, table_base_name, table_dt, next_table_dt, logged=False)
    create_table(conn, next_tablename, table_base_name, next_table_dt, next_next_table_dt, logged=False)
    sw.stop("table creation")

    # update status
    status_updater.current_file = 0
    status_updater.total_val = file_length

    bc = BinaryCopy()


    # start processing the file
    sw.start("db copy")
    while True:
        all_lines =  [(s.strip('\n'), json.loads(s)) for s in islice(infile, chunk_size)]

        if len(all_lines) == 0:
            break

        lines = [l for l in all_lines if verify_date_range(table_uts, next_table_uts, int(l[1]["created_utc"]))]
        overflow_lines = [l for l in all_lines if verify_date_range(next_table_uts, next_next_table_uts, int(l[1]["created_utc"]))]

        # check that everything falls in overflow

        if len(lines) + len(overflow_lines) != len(all_lines):
            print("!!! Some data was not within table or next (%d,%d,%d)" % (
                table_uts, next_table_uts, next_next_table_uts))
            for l in lines:
                if (verify_date_range(table_uts, next_table_uts, int(l[1]["created_utc"])) is False and
                    verify_date_range(next_table_uts, next_next_table_uts, int(l[1]["created_utc"])) is False):
                        print(l[1]["id"], l[1]["created_utc"])
            quit(1)


        # do the copy of valid data
        if table_base_name == "comments":
            bc.copy_comments(conn, tablename, lines)
            #copy_string_iterator_comment(conn, tablename, lines)

            # do copy of the misfiled data
            if len(overflow_lines) > 0:
                #copy_string_iterator_comment(conn, next_tablename, overflow_lines)
                bc.copy_comments(conn, tablename, lines)
        else:
            bc.copy_submissions(conn, tablename, lines)
            #copy_string_iterator_comment(conn, tablename, lines)

            # do copy of the misfiled data
            if len(overflow_lines) > 0:
                #copy_string_iterator_comment(conn, next_tablename, overflow_lines)
                bc.copy_submissions(conn, tablename, lines)

       
    


        # update status updater
        status_updater.count += len(all_lines)
        status_updater.total_added += len(all_lines)

        if file_length > 0:
            status_updater.current_val = os.lseek(
                infile.fileno(), 0, os.SEEK_CUR)
        status_updater.update()

    sw.stop("db copy")


    try:


        print("\tadding pk...")
        sw.start("add pk")
        primary_key_sql = "ALTER TABLE {} ADD PRIMARY KEY (id);".format(tablename)
        conn.execute(primary_key_sql)
        sw.stop("add pk")

        
        print("\tclustering...")
        sw.start("cluster")
        conn.execute("CLUSTER {} using {}_pkey".format(tablename, tablename))
        sw.stop("cluster")

        print("\tSetting logged...")
        sw.start("set logged")
        conn.execute(
            "ALTER TABLE {} SET LOGGED".format(tablename))
        sw.stop("set logged")


        print("\tattaching...")
        sw.start("add constraint")
        constraint_name = "y%04d_m%02d" % (table_dt.year, table_dt.month)
        start_date_str = "%04d-%02d-01 00:00:00" % (table_dt.year, table_dt.month)
        end_date_str = "%04d-%02d-01 00:00:00" % (next_table_dt.year, next_table_dt.month)
        conn.execute(
            "ALTER TABLE {} add constraint {} CHECK( created_utc >= TIMESTAMP '{}' and created_utc < TIMESTAMP '{}');".format(
                tablename, constraint_name, start_date_str, end_date_str ))
        sw.stop("add constraint")

        sw.start("attaching")
        conn.execute(
            "ALTER TABLE {} ATTACH PARTITION {} FOR VALUES FROM ('{}') to ('{}');".format(
                table_base_name,
                tablename,
                start_date_str,
                end_date_str
                ))
        sw.stop("attaching")


        print("\tadding index...")
        sw.start("index: brin(id)")
        id_brin_sql = "CREATE INDEX {}_id_brin_idx ON {} using BRIN (id);".format(tablename, tablename)
        conn.execute(id_brin_sql)
        sw.stop("index: brin(id)")


        sw.start("index: created_utc")
        date_index_sql = "CREATE INDEX ON {} (created_utc);".format(tablename)
        conn.execute(date_index_sql)
        sw.stop("index: created_utc")

        sw.start("index: brin(created_utc)")
        conn.execute(
            "CREATE INDEX {}_created_utc_brin_idx ON {} using BRIN (created_utc);".format(
                tablename, tablename))
        sw.stop("index: brin(created_utc)")


        sw.start("index: subreddit")
        
        conn.execute(
            "CREATE INDEX {}_subreddit_idx on {} (subreddit);".format(
                tablename, tablename))
        sw.stop("index: subreddit")

        sw.start("index: author")
        conn.execute(
            "CREATE INDEX {}_author_idx on {} (author);".format(
                tablename, tablename))
        sw.stop("index: author")



        #wait_select(conn)
        sw.start("commit")
        conn.commit()
        sw.stop("commit")


        print("\tvacuuming...")
        #conn.close()
        sw.start("vacuum")
        vacuum(conn, tablename)
        sw.stop("vacuum")





        
    except Exception as e:
        print("EXCEPTION: ", e)
        if conn is not None and conn.cursor is not None and conn.cursor.query is not None:
            print("QUERY: ", conn.cursor.query)
        else:
            print("NULL QUERY")
        traceback.print_exc()
        quit(1)


except Exception as e:
    print("EXCEPTION:", e)
    traceback.print_exc()
    traceback.print_stack()


finally:
    # make sure to close the file
    infile.close()
    #quit()

# commit the data
#conn.commit()
conn.close()
status_updater.update(force=True)

sw.stop("all")
sw.print_all()

print("\nCompleted successfully!")

            