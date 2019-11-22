"""
===============================================================================
# ConnectionWrapper
#
# A simple wrapper for psycopg
#
#===============================================================================
"""

import psycopg2
from psycopg2.extras import wait_select
import traceback
import getpass

class ConnectionWrapper():

    def __init__(self, host, port, database, username, password, async_conn=False):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.conn = None
        self.create_connection(async_conn=async_conn)
        self.async_conn = async_conn
        self.cursor = None

    def create_connection(self, async_conn=False):
        self.close()
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                async_=async_conn
                )
            self.async_conn = async_conn
        except Exception as e:
            print("failed to connect as '%s@%s' to database '%s'" % (
                self.username,
                self.host,
                self.database))
            traceback.print_exc()
            quit()


    def _ensure_cursor(self):
        if self.cursor is None:
            if self.async_conn:
                wait_select(self.conn)
            self.cursor = self.conn.cursor()


    def execute(self, query):
        self._ensure_cursor()

        if self.async_conn is True:
            wait_select(self.conn)

        self.cursor.execute(query)


    def commit(self):
        if self.conn is not None:
            if self.conn.closed is False:
                self.conn.commit()


    def close_cursor(self):
        if self.cursor is not None:
            if self.cursor.closed is False:
                if self.async_conn is True:
                    wait_select(self.conn)

                self.cursor.close()
            self.cursor = None

    def close(self):
        if self.conn is not None:
            self.close_cursor()

            if self.async_conn is True:
                wait_select(self.conn)
            else:
                self.conn.commit()

            if self.conn.closed is False:
                self.conn.close()
            self.conn = None


    def set_encoding(self, encoding):
        self.conn.set_client_encoding(encoding)



def get_connection(username, password, host, port, database, async_conn=False): 
    # set our username
    if username is None or len(username) == 0:
        username = getpass.getuser()

    # set our password
    #password = ""
    if not password:
        password = getpass.getpass(
            "Enter password for %s@%s (%s) : " % (
                username,
                host,
                database))


    return ConnectionWrapper(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            async_conn=async_conn
        )
