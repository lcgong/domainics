# -*- coding: utf-8 -*-

""" 对函数方法装配sqlblock，实例会增加sql属性，无异常结束提交事务，有异常则回滚.

@transaction

@transaction()

@transaction(auto_commit=True, dsn='other_db')

具体使用，必须提供第一个参数
@transaction()
def do_something(self, arg):
    ...
    return ...

如果该实例已经存在非空的sql属性则会抛出AttributeError

@transaction.sql_a

@transaction(dsn=)

"""

import logging
_logger = logging.getLogger(__name__)

import sys, functools, inspect
from itertools import chain as iter_chain

from ..domobj   import dobject
from ..sqltext import SQLText

import re
import asyncpg

from .dtable import dsequence


_conn_pools = {}
_datasources = {}

def set_dsn(dsn='DEFAULT', url=None, min_size=10, max_size=10):
    _datasources[dsn] = dict(dsn=url, min_size=min_size, max_size=max_size)

async def _get_pool(name):
    pool = _conn_pools.get(name)
    if pool is not None:
        return pool

    ds = _datasources.get(name)
    if ds is None:
        raise NameError('No dsn found: ' + name)

    try:
        pool = await asyncpg.create_pool(**ds)
        _conn_pools[name] = pool
        return pool
    except Exception as exc:
        _logger.error(str(exc))


async def close():
    await asyncio.gather(*(p.close() for p in _conn_pools.values()))



from collections import namedtuple

class RecordCursor:

    def __init__(self, sqlblk):
        self._sqlblock = sqlblk

        self._idx = None
        self._records = None
        self._n_records = None
        self._record_type = None

    async def execute(self):
        sql_stmt, sql_vals = self._sqlblock._sqltext.get_statment()
        if not sql_stmt:
            return

        stmt = await self._sqlblock._conn.prepare(sql_stmt)
        records = await stmt.fetch()
        if not records:
            self._idx = -1
            return

        self._attr_names = tuple(a.name for a in stmt.get_attributes())
        self._record_type = namedtuple("Record", self._attr_names)

        self._records = records
        self._n_records = len(records)
        self._idx = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._idx is None:
            await self.execute()

        idx = self._idx
        if idx < 0 or idx >= self._n_records:
            self._idx = None
            self._records = None
            self._n_records = None
            self._record_type = None
            raise StopAsyncIteration()

        self._idx += 1
        return self._record_type(*self._records[idx])

    def __iter__(self):
        return self

    def __next__(self):
        if self._idx is None:
            raise StopIteration

        idx = self._idx
        if idx < 0 or idx >= self._n_records:
            self._idx = None
            self._records = None
            self._n_records = None
            self._record_type = None
            raise StopIteration()

        self._idx += 1
        return self._record_type(*self._records[idx])

    def __dset__(self, item_type):

        dobj_attrs = OrderedDict((attr_name, attr) for attr_name, attr in
                            iter_chain(item_type.__dobject_key__.items(),
                                       item_type.__dobject_att__.items()))

        colnames = []
        selected = []
        for i, attr_name in enumerate(self._attr_names):
            colname = d[0]
            if colname in dobj_attrs:
                selected.append(i)
                colnames.append(attr_name)

        records = self.records
        for i in range(self._n_records):
            row = records[0]

            obj = dict((k, v)
                       for k, v in zip(colnames, (record[i] for i in selected)))
            yield item_type(**obj)

class BaseSQLBlock:
    def __init__(self, dsn='DEFAULT', record_type=None):

        # self._record_type = record_type or _default_record_type

        self.dsn = dsn
        self._cursor = RecordCursor(self)
        self._sqltext = SQLText()


    async def __enter__(self):
        pool = await _get_pool(self.dsn)
        if pool:
            self._conn = await pool.acquire()
            self._transaction = self._conn.transaction()
            await self._transaction.start()

        return self

    async def __exit__ (self, etyp, exc_val, tb):
        if exc_val :
            await self._transaction.rollback()
        else:
            await self._transaction.commit()

        if self._conn:
            pool = await _get_pool(self.dsn)
            await pool.release(self._conn)
            self._conn = None

        return False

    def __lshift__(self, sqltext):
        self._sqltext = SQLText()
        self._sqltext._join(sqltext, frame=sys._getframe(1))
        return self

    async def execute(self, **parameters):

        self._cursor = RecordCursor(self)
        await self._cursor.execute()

    def __aiter__(self):
        return  self._cursor or RecordCursor(self)

    def __iter__(self):
        if self._cursor._records is None:
            raise Exception('before iterating, should wait for execute()')

        return  self._cursor

    def __dset__(self, item_type):
        if self._cursor:
            return self._cursor.__dset__(item_type)

        return dset(item_type)()

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

class TransactionDecorator:

    def __init__(self):
        self.alias = None

    def __getattr__(self, alias): # new decorator for a new alias
        if self.alias is not None:
            raise TypeError('the variable is defined: ' + self.alias)

        transaction = TransactionDecorator()
        transaction.alias = alias

        return transaction

    def __call__(self, *d_args, dsn='DEFAULT'):
        if self.alias is None:
            raise TypeError('The connection variable name is required')


        def _decorator(func):

            func_sig = inspect.signature(func)
            if self.alias not in func_sig.parameters:
                raise TypeError(f"The argument '{self.alias}' should be declared"
                                f" in {func.__name__} in {func.__module__}")

            sqlblk = BaseSQLBlock(dsn=dsn)
            newfunc = functools.partial(func, **{self.alias: sqlblk})

            async def _sqlblock_wrapper(*args, **kwargs):
                await sqlblk.__enter__()
                try:
                    return await newfunc(*args, **kwargs)
                finally:
                    await sqlblk.__exit__(*sys.exc_info())


            functools.update_wrapper(_sqlblock_wrapper, newfunc)
            return _sqlblock_wrapper

        if len(d_args) == 1 and callable(d_args[0]):
            # no argument decorator
            return _decorator(d_args[0])
        else:
            return _decorator


transaction = TransactionDecorator()
