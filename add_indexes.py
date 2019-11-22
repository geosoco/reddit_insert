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
parser.add_argument('-u', '--user', help="username")
parser.add_argument('-p', '--password', help="password", action="store_true")


args = parser.parse_args()

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
        password=user_password
    )


skip_list = set(["comments_y2015_m01"])

#
# Begin Database Connection
#
conn.execute("SET TIME ZONE 'UTC';")
psycopg2.extras.register_default_json(loads=lambda x: x)


status_updater = StatusUpdater()
status_updater.total_files = 1

QUERY_GET_TABLES = """
SELECT table_name FROM information_schema.tables 
WHERE table_schema = 'public' and table_name similar to '(comments|submissions)_y%'
order by table_name asc;
"""

conn.execute(QUERY_GET_TABLES)

table_names = [table[0] for table in conn.cursor]
status_updater.total_files = len(table_names)


SUBREDDIT_INDEX = """Create index if not exists {}_subreddit_idx on {} ((data->>'subreddit'))""";
AUTHOR_INDEX = """CREATE INDEX if not exists {}_author_idx on {} (author)""";
VACUUM_QUERY = """VACUUM full analyze {}"""

for table in table_names:
    start = datetime.now()
    print datetime.now(), table, "..."
    if table not in skip_list:
        conn.execute(SUBREDDIT_INDEX.format(table, table))
        print "\tfinished subreddit index: ", datetime.now() - start
        start_author = datetime.now()
        conn.execute(AUTHOR_INDEX.format(table, table))
        print "\tfinished author index: ", datetime.now() - start_author
        #old_isolation_level = conn.conn.isolation_level
        #conn.conn.set_isolation_level(0)
        #conn.execute(VACUUM_QUERY.format(table))
        #conn.conn.set_isolation_level(old_isolation_level)
        conn.conn.commit()
    else:
        print "\t<<skipping>>"
    print "\tdone: ", datetime.now() -start

