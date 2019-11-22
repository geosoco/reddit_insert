#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

import argparse
from utils.db_helpers import ConnectionWrapper, get_connection
from utils.iterhelpers import StringIteratorIO
from itertools import islice

from io import BytesIO, BufferedRandom, StringIO
import numpy as np
from datetime import datetime
from typing import Optional, Any, Dict, Iterator, Tuple

import ujson as json
import psycopg2
import re
import traceback

import bz2

import cProfile
import pstats

import random

from struct import pack, Struct



json_null_tests = [
    r"""{"archived":false,"subreddit":"startups","author_flair_css_class":null,"controversiality":0,"gilded":0,"author_flair_text":null,"score_hidden":false,"retrieved_on":1424633238,"distinguished":null,"score":4,"parent_id":"t1_cont3y1","link_id":"t3_2w5kh7","body":"Internet music discovery is dominated by platforms expanding their music libraries as quickly as possible, creating an information overload and decision paralysis. It's really hard to find the newest, best music simply because they offer too many songs. Noon Pacific serves up 10 songs each week for you to explore and listen to as if it were a vinyl record. \n\nAlso, almost every music discovery tool I know of is influenced by record companies or money-backed singles so the charts are really skewed.\n\nCurrent monetization is via the mobile apps at $1.99 for iOS + Android.\n\u0000","author":"cdinnison","id":"conuz8i","name":"t1_conuz8i","created_utc":"1424150044","subreddit_id":"t5_2qh26","edited":false,"downs":0,"ups":4}""",
    r"""{"controversiality":0,"edited":false,"archived":false,"parent_id":"t1_con72jc","author":"herpy","body":"Thank you.  I buried my dreams along time ago before I figured out all this stuff about ADHD and found this sub.  But after finding it and reading stories like the one in this post I felt like those dreams were trying to rise from the grave I put them  in because maybe I was wrong and there was another way.  I think you just helped seal them for good.  Damn zombie dreams.\u0000","ups":1,"downs":0,"retrieved_on":1424622706,"distinguished":null,"created_utc":"1424110059","name":"t1_con8u0k","id":"con8u0k","author_flair_text":null,"subreddit_id":"t5_2qnwb","gilded":0,"author_flair_css_class":null,"score":1,"subreddit":"ADHD","score_hidden":false,"link_id":"t3_2w0iw2"}""",
    r"""{"name":"t1_co98h70","link_id":"t3_2ukm41","subreddit":"cs50","body":"It will be graded by check50 so if you submit it, you won't pass.\n\nLook at your error messages.  Notice how you are returning something at the end of your output?  `\\u0000` ?  That's an unprintable char which is why you don't see it on your output.  Check your loops again to make sure you aren't printing one too many characters...","author":"delipity","controversiality":0,"downs":0,"created_utc":"1422920336","author_flair_text":"alum","distinguished":null,"parent_id":"t1_co97tks","id":"co98h70","score":3,"archived":false,"ups":3,"subreddit_id":"t5_2s3yg","retrieved_on":1424247157,"edited":false,"gilded":0,"score_hidden":false,"author_flair_css_class":null}""",
    r"""{"archived":false,"created_utc":"1422878204","author_flair_text":null,"score_hidden":false,"subreddit_id":"t5_2t1jq","controversiality":0,"gilded":0,"edited":false,"author":"OldNedder","subreddit":"javahelp","distinguished":null,"body":"close - you probably need \"r &lt; height\" to go through the whole grid.  It seems like the longColumn parameter is unnecessary.  width and height can be obtained from the grid itself (unless you are allocating a grid with unused space).\n\nAlso \\u0000 should be '\\u0000'.  Or you can just compare the character with the integer 0:  if (grid[r][c] != 0) {}","author_flair_css_class":null,"score":1,"parent_id":"t1_co83juk","name":"t1_co8n1jx","id":"co8n1jx","link_id":"t3_2udofx","ups":1,"retrieved_on":1424257326,"downs":0}"""

]



POSTGRES_EPOCH = 946684800


def grouper(n, iterable):
    it = iter(iterable)
    while True:
       chunk = tuple(islice(it, n))
       if not chunk:
           return
       yield chunk


def hex_view(m):
    return ' '.join([hex(b)[2:].zfill(2) for b in m])

def char_view(m):
    return ''.join([chr(b) if b >= 32 and b < 126 else '.'  for b in m])

def hexdump(m,n=16):
    print('\n'.join([hex_view(a).ljust(n*2+n-1) + "  " + char_view(a) for a in grouper(n,m)]))



def create_table(conn, table, unlogged=True):
    # remake the table
    conn.execute("drop table if exists {};".format(table))
    conn.execute("""create {} table {} (
            id bigint not null,
            created_utc timestamp without time zone, 
            article bigint,
            author varchar(32),
            subreddit varchar(48),
            data jsonb
        );""".format("unlogged" if unlogged is True else "", table))



def timestamp_to_pgtimestamp(ts):
    return (ts - POSTGRES_EPOCH) * 1000000


def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n').replace('\0', '<<:NULL:>>')



def get_article(article: str) -> int:
    #article = int(article[3:],36) if article.startswith("t3") else int(article)
    article = int(article[3:],36) if article is not None and article[:2] == 't3' else None
    return article


def get_parent(id: str) -> Any:
    return int(id[3:], 36) if id is not None and id[:2] == 't1' else None


#def clean_json_string(js: str) -> str:


def fix_unicode_string(line):
    s = re.sub(r"(?<!\\)\\u0000", " ", line)
    s = re.sub("\00", "<<:NULL:>>", s)

    return s


def test_string_iterator1(rows: Iterator[Tuple[str,Dict[str, Any]]], size: int = 8192) -> None:
    for row in rows:
        s = '|'.join(
            map(
                clean_csv_value, 
                (
                    int(row[1]["id"], 36),
                    datetime.utcfromtimestamp(int(row[1]["created_utc"])),
                    get_article(row[1]["link_id"]),
                    row[1]["author"],
                    row[1]["subreddit"],
                    fix_unicode_string(row[0])
                )
            )
        ) + '\n'

        print(s)



def copy_string_iterator(conn, table, rows: Iterator[Tuple[str,Dict[str, Any]]], size: int = 8192) -> None:
    try:
        string_iter = StringIteratorIO((
            '|'.join(
                map(
                    clean_csv_value, 
                    (
                        int(row[1]["id"], 36),
                        datetime.utcfromtimestamp(int(row[1]["created_utc"])),
                        get_article(row[1]["link_id"]),
                        row[1].get("author", None),
                        row[1].get("subreddit", None),
                        '"' + fix_unicode_string(row[0].replace('"', '""')) + '"'
                        )
                    )
                ) + '\n'
            for row in rows

            ))

        conn.cursor.copy_expert("copy %s from stdin with csv freeze delimiter as '|' " % (table), string_iter )
    except Exception as e:
        for row in rows:
            print('\n'.join(
                map(
                    clean_csv_value, 
                    (
                        int(row[1]["id"], 36),
                        datetime.utcfromtimestamp(int(row[1]["created_utc"])),
                        get_article(row[1]["link_id"]),
                        row[1].get("author", None),
                        row[1].get("subreddit", None),
                        '"' + row[0].replace('"', '""').replace('\0', '<<:NULL:>>') + '"'
                        )
                    )
                ) + '\n'*3)

        print("-----")

        print(e)




def create_insert_tuple(line, obj):
    corrected_line = re.sub(r"(?<!\\)\\u0000", " ", line)
    corrected_line = re.sub("\00", "<<:NULL:>>", corrected_line)
    #if '\\u000' in corrected_line or '\u0000' in corrected_line:
    #    print "---- str still has null"
    #    print corrected_line
    #    print obj

    try:
        timestamp = int(obj["created_utc"])
        dt = datetime.utcfromtimestamp(timestamp)
        id_int = int(obj["id"], 36)

        t = (
            int(obj["id"], 36),
            dt,
            obj.get("author", None),
            obj.get("subreddit", None),
            get_article(obj["link_id"]),
            corrected_line)

        return t
    except KeyError as e:
        print("key error")
        print(json.dumps(obj, indent=4))
        raise e
    except Exception as e:
        print("Exception", e)
        traceback.print_exc()
        raise e



def do_insert(conn, table, lines, chunk_size):
    arg_list = u"(%s, %s, %s, %s, %s, %s)"
    db_cols = u"(id, created_utc, author, subreddit, article, data)"

    lines_iter = iter(lines)

    while True:
        all_lines = [create_insert_tuple(i[0], i[1]) for i in islice(lines_iter, chunk_size )]

        if len(all_lines) == 0:
            break
        #print(all_lines[0])

        try:
            values = []
            query = None
            if len(all_lines) > 0:
                #print([conn.cursor.mogrify(arg_list, x) for x in all_lines])
                values = ','.join(
                    conn.cursor.mogrify(arg_list, x).decode('utf-8') for x in all_lines)
                #print("values", values)
                query = "insert into %s %s values " % (
                    table, db_cols)
                conn.execute(query + values)

        except Exception as e:
            print("EXCEPTION: ", e)
            traceback.print_exc()
            print("-" * 78)

            print(query)
            #for i, v in enumerate(all_lines):
            #    print("<>"*30, "\n", i,"\n", v, "\n\n")
            #print(values)
            quit()




def write_binary_header(fs):
    fs.write(pack('!11sii', b'PGCOPY\n\xff\r\n\0', 0, 0))


def get_binary_row_string_data(row):
    auth = row[1]["author"].encode()
    sub = row[1]["subreddit"].encode()
    data = row[0].encode()

    return (auth, sub, data)    


_row_header_struct = Struct("!hiqiq")
_row_int_struct = Struct("!i")

_row_null_val = pack("!i", -1)


def write_binary_string(fs, obj, keyname):
    try:
        val = obj[keyname]

        if val is None:
            fs.write(_row_null_val)
        else:
            val = val.encode()
            fs.write(_row_int_struct.pack(len(val)))
            fs.write(val)
    except KeyError as e:
        fs.write(_row_null_val)





def write_binary_row2(fs, row):
    fs.write(_row_header_struct.pack(6,
        8, int(row[1]["id"], 36), 
        8, timestamp_to_pgtimestamp(int(row[1]["created_utc"]))
        ))

    # write article id
    aid = get_article(row[1]["link_id"])
    if aid is None:
        fs.write(_row_null_val)
    else:
        fs.write(pack("!iq", 8, aid))


    # write strings
    write_binary_string(fs, row[1], "author")
    write_binary_string(fs, row[1], "subreddit")

    data = row[0].encode()
    fs.write(pack("!ib", len(data) +1, 1))
    fs.write(data)






def write_binary_row(fs, row):
    #fs.write(pack('!h', 6))

    #auth = bytes(row[1].get("author", None), "UTF8")
    #sub = bytes(row[1].get("subreddit", None), "UTF8")
    #data = bytes(row[0], "UTF8")

    #auth = bytes(row[1]["author"], "UTF8")
    #sub = bytes(row[1]["subreddit"], "UTF8")
    #data = bytes(row[0], "UTF8")

    auth, sub, data = get_binary_row_string_data(row)

    alen = len(auth)
    slen = len(sub)
    dlen = len(data)

    fmt = "!hiqiqiqi%isi%isib" %(alen, slen)

    """
    print(fmt)
    hexdump(pack(fmt,
        6, 
        8, int(row[1]["id"], 36), 
        8, timestamp_to_pgtimestamp(int(row[1]["created_utc"])),
        len(auth), auth,
        len(sub), sub,
        8, get_article(row[1]["link_id"]),
        len(data)+1, 1
    ))"""

    #fs.write(pack('!iqiq',

    fs.write(pack(fmt,
        6, 
        8, int(row[1]["id"], 36), 
        8, timestamp_to_pgtimestamp(int(row[1]["created_utc"])),
        8, get_article(row[1]["link_id"]),
        alen, auth,
        slen, sub,
        dlen+1, 1
    ))
    #fs.write(pack('!iq', 8, ))
    
    """
    fs.write(pack('!i', len(auth)))
    fs.write(auth)
    
    fs.write(pack('!i', len(sub)))
    fs.write(sub)
    fs.write(pack('!iq', 8, get_article(row[1]["link_id"])))
    
    fs.write(pack('!ib', len(data)+1, 1))
    """
    fs.write(data)


def do_binary_copy(conn, table, lines, chunk_size):

    with BufferedRandom(BytesIO(), buffer_size=(256 * 1024 * 1024)) as fs:
    #fs = BytesIO()
    
        lines_iter = iter(lines)

        while True:
            all_lines = list(islice(lines_iter, chunk_size ))

            if len(all_lines) == 0:
                break

            write_binary_header(fs)

            for l in all_lines:
                write_binary_row(fs, l)

            # write end of task
            fs.write(pack('!h', -1))
            fs.flush()
            fs.seek(0)

            #hexdump(fs.raw.getbuffer())

            conn.cursor.copy_expert("copy %s from stdin with binary freeze" % (table), fs )
            fs.seek(0)
            fs.truncate()



def do_binary_copy2(conn, table, lines, chunk_size):
    with BufferedRandom(BytesIO(), buffer_size=(256 * 1024 * 1024)) as fs:
    #fs = BytesIO()
    
        lines_iter = iter(lines)

        while True:
            all_lines = list(islice(lines_iter, chunk_size ))

            if len(all_lines) == 0:
                break

            write_binary_header(fs)

            for l in all_lines:
                write_binary_row2(fs, l)

            # write end of task
            fs.write(pack('!h', -1))
            fs.flush()
            fs.seek(0)

            #hexdump(fs.raw.getbuffer())

            conn.cursor.copy_expert("copy %s from stdin with binary freeze" % (table), fs )
            fs.seek(0)
            fs.truncate()




def profile_test(conn, table, lines, method, name, total_size, chunk_size, unlogged):
    # create the table
    create_table(conn, table, unlogged)

    # now start the timer
    dt_start = datetime.now()


    #pr = cProfile.Profile()
    #pr.enable()

    method(conn,table,lines, chunk_size)
    

    """
    pr.disable()
    s = StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats(pstats.SortKey.CUMULATIVE)
    ps.print_stats()
    print(s.getvalue())
    """

    diff = datetime.now() - dt_start
    diff_str = str(diff)

    # close the cursor to make sure
    conn.close_cursor()

    print("-"*50)
    print("Results for %s  (chunksize: %d, unlogged: %s): %.4f (%.4f rows/sec)" % (name, chunk_size, str(unlogged), diff.total_seconds(), (float(total_size)/float(diff.total_seconds()))))
    print("-"*50)
    print("\n"*2)



#
# PARSER
#

parser = argparse.ArgumentParser(description="reddit json importer")
parser.add_argument('--host', default="localhost", help='host')
parser.add_argument('--port', default=5432, help='host port', type=int)
parser.add_argument('database', help='database name')
parser.add_argument('table', help="table name")
parser.add_argument('filename', help='filename to import')
parser.add_argument('-u', '--user', help="username")
#parser.add_argument('-p', '--password', help="password", action="store_true")
parser.add_argument('-p', '--password', help="password")


# parse
args = parser.parse_args()





# build a connection
conn = get_connection(args.user, args.password, args.host, args.port, args.database, async_conn=False)


NUM_LINES = 100000


#
# Begin Database Connection
#
conn.execute("SET TIME ZONE 'UTC';")
#conn.set_encoding("UTF8")
# leave json as a string when reading
psycopg2.extras.register_default_json(loads=lambda x: x)


cases = [
    {"name": "copy binary", "bz2": False, "method":do_binary_copy, "chunk_size": 64, "unlogged": False},
    {"name": "copy binary", "bz2": False, "method":do_binary_copy, "chunk_size": 256, "unlogged": False},
    {"name": "copy binary", "bz2": False, "method":do_binary_copy, "chunk_size": 1024, "unlogged": False},
    #{"name": "copy binary", "bz2": False, "method":do_binary_copy, "chunk_size": 4096, "unlogged": False},
    {"name": "copy binary", "bz2": False, "method":do_binary_copy, "chunk_size": 64, "unlogged": True},
    {"name": "copy binary", "bz2": False, "method":do_binary_copy, "chunk_size": 256, "unlogged": True},
    {"name": "copy binary", "bz2": False, "method":do_binary_copy, "chunk_size": 1024, "unlogged": True},
    #{"name": "copy binary", "bz2": False, "method":do_binary_copy, "chunk_size": 4096},

    {"name": "copy binary 2", "bz2": False, "method":do_binary_copy2, "chunk_size": 64, "unlogged": False},
    {"name": "copy binary 2", "bz2": False, "method":do_binary_copy2, "chunk_size": 256, "unlogged": False},
    {"name": "copy binary 2", "bz2": False, "method":do_binary_copy2, "chunk_size": 1024, "unlogged": False},
    #{"name": "copy binary 2", "bz2": False, "method":do_binary_copy2, "chunk_size": 4096, "unlogged": False},
    {"name": "copy binary 2", "bz2": False, "method":do_binary_copy2, "chunk_size": 64, "unlogged": True},
    {"name": "copy binary 2", "bz2": False, "method":do_binary_copy2, "chunk_size": 256, "unlogged": True},
    {"name": "copy binary 2", "bz2": False, "method":do_binary_copy2, "chunk_size": 1024, "unlogged": True},
    #{"name": "copy binary 2", "bz2": False, "method":do_binary_copy2, "chunk_size": 4096, "unlogged": False},

    {"name": "copy csv", "bz2": False, "method":copy_string_iterator, "chunk_size": 64, "unlogged": False},
    {"name": "copy csv", "bz2": False, "method":copy_string_iterator, "chunk_size": 256, "unlogged": False},
    {"name": "copy csv", "bz2": False, "method":copy_string_iterator, "chunk_size": 1024, "unlogged": False},
    #{"name": "copy csv", "bz2": False, "method":copy_string_iterator, "chunk_size": 4096, "unlogged": False},
    {"name": "copy csv", "bz2": False, "method":copy_string_iterator, "chunk_size": 64, "unlogged": True},
    {"name": "copy csv", "bz2": False, "method":copy_string_iterator, "chunk_size": 256, "unlogged": True},
    {"name": "copy csv", "bz2": False, "method":copy_string_iterator, "chunk_size": 1024, "unlogged": True},
    #{"name": "copy csv", "bz2": False, "method":copy_string_iterator, "chunk_size": 4096, "unlogged": False},

#    {"name": "insert", "bz2": False, "method":do_insert},
]


def run_case(case, total_size=100000):
    with open(args.filename, "r") as infile:
        lines = ((s.strip('\n'), json.loads(s)) for s in islice(infile, total_size))
        profile_test(conn, args.table, lines, case["method"], case["name"], total_size, case["chunk_size"], case["unlogged"])


def run_all_cases(total_size=NUM_LINES):
    random.shuffle(cases)

    for c in cases:
        run_case(c, total_size=total_size)


def test_null(case):
    lines = [(s.strip('\n'), json.loads(s)) for s in json_null_tests]
    profile_test(conn, args.table, lines, case["method"], case["name"])


run_all_cases()


#test_null(cases[1])


#run_case(cases[0])
#cProfile.run("run_case(cases[0])")



"""
with open(args.filename, "r") as infile:
    lines = ((s.strip('\n'), json.loads(s)) for s in islice(infile, NUM_LINES))
    profile_test(conn, args.table, lines, do_binary_copy, "copy binary")

with open(args.filename, "r") as infile:
    lines = ((s.strip('\n'), json.loads(s)) for s in islice(infile, NUM_LINES))
    
    profile_test(conn, args.table, lines, do_insert, "insert")


with open(args.filename, "r") as infile:
    lines = ((s.strip('\n'), json.loads(s)) for s in islice(infile, NUM_LINES))

    profile_test(conn, args.table, lines, copy_string_iterator, "copy string iterator")


with bz2.BZ2File("RC_2013-04.bz2", "r") as infile:
    lines = ((s.decode('utf8').strip('\n'), json.loads(s)) for s in islice(infile, NUM_LINES))
    print(type(lines))

    profile_test(conn, args.table, lines, do_insert, "bz2 insert")


with bz2.BZ2File("RC_2013-04.bz2", "r") as infile:
    lines = ((s.decode('utf8').strip('\n'), json.loads(s)) for s in islice(infile, NUM_LINES))

    profile_test(conn, args.table, lines, copy_string_iterator, "bz2 copy string iterator")
"""

conn.close()

