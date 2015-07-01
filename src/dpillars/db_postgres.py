# -*- coding: utf-8 -*-

import logging
_logger = logging.getLogger(__name__)


import psycopg2.pool
import psycopg2.extras
import psycopg2.extensions
from psycopg2.pool import ThreadedConnectionPool
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

from tornice.db import BaseSQLBlock


class PostgreSQLBlock(BaseSQLBlock):
    _conn_pools = {}

    @classmethod
    def set_dsn(cls, **kwargs):
        dsn = kwargs.pop('dsn', 'DEFAULT')
        kwargs.setdefault('minconn', 1)
        kwargs.setdefault('maxconn', 10)

        cls._conn_pools[dsn] = ThreadedConnectionPool(**kwargs)

    def _open(self):
        pool = self._conn_pools[self.dsn]
        self._conn   = pool.getconn()
        self._conn.autocommit = self.autocommit
        self._cursor = self._conn.cursor()

    def _close(self):
        if self._cursor: 
            self._cursor.close()
            self._cursor = None

        if self._conn:
            pool = self._conn_pools[self.dsn]
            pool.putconn(self._conn)
            self._conn = None

    def nextval(self, seq, batch_cnt=None):
        cur = self.cursor
        if batch_cnt is None :
            s = "SELECT nextval(%(seq)s)"
            cur.execute(s, dict(seq=seq))
            row = next(cur.fetchall())
            return row[0] if row is not None else None

        s = "SELECT nextval(%(seq)s) FROM generate_series(1, %(cnt)s) s"
        cur.execute(s, dict(seq=seq, cnt=batch_cnt))
        return (r[0] for r in  cur.fetchall())
    
