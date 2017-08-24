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


from inspect import Signature

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

        self._paramerters = None

        self._idx = None
        self._records = None
        self._n_records = None
        self._record_type = None

    async def execute(self):
        sqltext = self._sqlblock._sqltext
        self._sqlblock._sqltext = SQLText()

        sql_stmt, sql_vals, is_many = sqltext.get_statment(self._paramerters)
        if not sql_stmt:
            return

        if is_many:
            await self._sqlblock._conn.executemany(sql_stmt, sql_vals)
            self._idx = -1
            return

        stmt = await self._sqlblock._conn.prepare(sql_stmt)
        records = await stmt.fetch(*sql_vals)
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
        if self._sqlblock._sqltext:
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
    __tuple__ = ('dsn', '_sqltext', '_cursor',
                 '_parent_sqlblk', '_decorated_func')

    def __init__(self, dsn='DEFAULT', parent=None, decorated_func=None):
        self.dsn = dsn
        self._parent_sqlblk = parent
        self._decorated_func = decorated_func

        self._cursor = RecordCursor(self)
        self._sqltext = SQLText()


    async def __enter__(self):
        if self._parent_sqlblk:
            self._conn = self._parent_sqlblk._conn
            return self

        pool = await _get_pool(self.dsn)
        if pool:
            self._conn = await pool.acquire()
            self._transaction = self._conn.transaction()
            await self._transaction.start()

        return self

    async def __exit__ (self, etyp, exc_val, tb):
        if self._parent_sqlblk:
            return False

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
        self._sqltext._join(sqltext, frame=sys._getframe(1))
        return self

    async def execute(self, **parameters):
        self._cursor._paramerters = parameters
        await self._cursor.execute()

    async def executemany(self, parameters_list):
        self._cursor._paramerters = parameters_list
        await self._cursor.execute()

    def __aiter__(self):
        return  self._cursor

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

    def __repr__(self):

        if self._decorated_func:
            func_str = f"against '{self._decorated_func.__name__}' "
            func_str += f"in '{self._decorated_func.__module__}'"
        else:
            func_str = ""

        return f"<SQLBlock dsn='{self.dsn}' " + func_str +  f" at 0x{id(self):x}>"

class TransactionDecoratorFactory:
    def __init__(self):
        self.dsn = None

    def __getattr__(self, dsn):
        return TransactionDecorator(dsn)
        # if self.dsn is not None:
        #     raise TypeError(f"tranaction decorator has already "
        #                     f"set a dsn '{self.dsn}'")

    def __call__(self, *args, kwargs):
        raise TypeError(f"usage: such as 'transaction.db', 'db; is datasource name")


class TransactionDecorator:
    __tuple__ = ('dsn')

    def __init__(self, dsn):
        self.dsn = dsn

    def __call__(self, *d_args):
        if self.dsn is None:
            raise TypeError('The dsn agasint this transaction is required')

        _self_dsn = self.dsn

        def _decorator(func):
            func_sig = inspect.signature(func)
            if _self_dsn not in func_sig.parameters:
                raise TypeError(f"The parameter '{_self_dsn}' is required "
                                f" in {func.__name__} in {func.__module__}")

            if _self_dsn.startswith('_dsn_'): # The referrence of dsn
                _dsn_param = func_sig.parameters[_self_dsn]
                if (_dsn_param.default is Signature.empty
                    or _dsn_param.default is not None):

                    raise TypeError(f"The parameter '{_self_dsn}' should "
                                    f"declare that '{_self_dsn}=None' "
                                    f"in {func.__name__} in {func.__module__} ")
                newfunc = func
            else: # The normal dsn
                newfunc = functools.partial(func, **{_self_dsn: None})
                functools.update_wrapper(newfunc, func)

            async def _sqlblock_wrapper(*args, **kwargs):
                if _self_dsn.startswith('_dsn_'):
                    _dsn_var = kwargs.get(_self_dsn)
                    if _dsn_var is not None:
                        if isinstance(_dsn_var, BaseSQLBlock):
                            _parent_sqlblk = _dsn_var
                        elif isinstance(_dsn_var, str):
                            _parent_sqlblk = _find_parent_sqlblock(_dsn_var)
                            if _parent_sqlblk is None:
                                raise ValueError(
                                    f"no found the parent sqlblock(dsn='{_dsn_var}')"
                                    f" against '{_self_dsn}' while calling"
                                    f" '{newfunc.__name__}' of {newfunc.__module__}")
                        else:
                            raise ValueError(
                                f"Unknown the value of '{_self_dsn}'"
                                f": {str(_dsn_var)}")
                    else:
                        _parent_sqlblk = _find_parent_sqlblock(None)

                    if _parent_sqlblk is None:
                        raise ValueError(f"Cannot find the parent sqlblock"
                                         f"agasint '{_self_dsn}'. ")

                else:
                    _parent_sqlblk = _find_parent_sqlblock(_self_dsn)

                __sqlblk_obj = BaseSQLBlock(dsn=_self_dsn,
                                            parent=_parent_sqlblk,
                                            decorated_func=newfunc)
                kwargs[_self_dsn] = __sqlblk_obj

                await __sqlblk_obj.__enter__()
                try:
                    return await newfunc(*args, **kwargs)
                finally:
                    await __sqlblk_obj.__exit__(*sys.exc_info())

            functools.update_wrapper(_sqlblock_wrapper, newfunc)
            return _sqlblock_wrapper

        if len(d_args) == 1 and callable(d_args[0]): # no argument decorator
            return _decorator(d_args[0])
        else:
            return _decorator

def _find_parent_sqlblock(dsn):
    frame = sys._getframe(2)
    while frame:
        sqlblk = frame.f_locals.get('_TransactionDecorator__sqlblk_obj')
        if sqlblk and (not dsn or sqlblk.dsn == dsn):
            return sqlblk

        frame = frame.f_back
    return None

transaction = TransactionDecoratorFactory()
