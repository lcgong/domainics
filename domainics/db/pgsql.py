# -*- coding: utf-8 -*-

import logging
_logger = logging.getLogger(__name__)

import re
from psycopg2.pool import ThreadedConnectionPool


from .sqlblock import BaseSQLBlock
from .dtable import dsequence


import psycopg2.extensions as _pgext 
# _pgext.register_type(_pgext.UNICODE)
# _pgext.register_type(_pgext.UNICODEARRAY)

# register postgresql type dsequence
_pgext.register_adapter(dsequence, lambda seq : _pgext.AsIs(repr(seq.value)))

class jsonb:
    pass


class PostgreSQLBlock(BaseSQLBlock):
    _conn_pools = {}

    def __init__(self, dsn='DEFAULT', autocommit=False, record_type=None):
        super(PostgreSQLBlock, self).__init__('postgres', dsn, autocommit, record_type)

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
        cur = self._cursor
        if batch_cnt is None :
            s = "SELECT nextval(%(seq)s)"
            cur.execute(s, dict(seq=seq))
            row = next(cur.fetchall())
            return row[0] if row is not None else None

        s = "SELECT nextval(%(seq)s) FROM generate_series(1, %(cnt)s) s"
        cur.execute(s, dict(seq=seq, cnt=batch_cnt))
        return (r[0] for r in  cur.fetchall())
    

    @staticmethod
    def _has_params(sqlstmt):
        return bool(_ptn_sqlparams.search(sqlstmt))


_ptn_sqlparams = re.compile(r'%(?:s|\(\w+\)s)')

