# -*- coding: utf-8 -*-

import logging

import functools
from collections  import OrderedDict
from collections  import namedtuple  as _namedtuple
from itertools import chain as iter_chain
from abc import abstractmethod

from ..util     import nameddict   as _nameddict
from ..pillar   import _pillar_history, pillar_class, PillarError, History
from ..domobj   import dobject

_dsn_class = {}

def set_dsn(**kwargs):
    ''' '''
    dbsys = kwargs.pop('sys', 'postgres')
    dsn   = kwargs.get('dsn', 'DEFAULT')
    if dbsys in ('postgres', 'pgsql', 'pg') :
        from . import pgsql as _pg

        _dsn_class[dsn] = _pg.PostgreSQLBlock
        _pg.PostgreSQLBlock.set_dsn(**kwargs)

    elif dbsys in ('sqlite'):
        raise NotImplemented('sqlite')

    elif dbsys in ('mysql'):
        raise NotImplemented('mysql')
    else:
        raise ValueError('unkown DBMS: ' + dbsys)

def sqlblock(dsn='DEFAULT', autocommit=False, record_type=None):

    sqlblock_class = _dsn_class[dsn]

    blkobj = sqlblock_class(
        dsn=dsn,
        autocommit=autocommit,
        record_type=record_type)

    return blkobj

def record_dict(cursor):
    fields = (d[0] for d in cursor.description)
    for row in cursor:
        yield OrderedDict(zip(fields, row))

def record_namedtuple(cursor):
    dt = _namedtuple("Row", [d[0] for d in cursor.description or ()])
    for row in cursor:
        yield dt(*row)

def record_plainobj(cursor):
    dt = _nameddict("Row", [d[0] for d in cursor.description or ()])
    for row in cursor:
        yield dt(*row)

def make_record_dtable(dobj_cls):

    attrnames = set(dobj_cls._dobj_attrs.keys())

    def record_dtable(cursor):
        nonlocal attrnames

        fields = [d[0] for d in cursor.description or ()]

        colnames, colidxs = [], []
        for i, f in enumerate(fields):
            if f not in attrnames:
                continue
            colnames.append(f)
            colidxs.append(i)


        dt = _nameddict("Row", [d[0] for d in cursor.description or ()])
        # for row in cursor:
        #     yield dobj_cls(**zip(colnames, (row[i] for i in colidxs))

class BaseSQLBlock:
    def __init__(self, dbms, dsn='DEFAULT', autocommit=False, record_type=None):

        self._record_type = record_type or _default_record_type

        self.dbms = dbms
        self.dsn = dsn
        self.autocommit = autocommit

        self._cur_record_type = None
        self.__todo_sqlstmt   = None

        self._logger = logging.getLogger(__name__)

    def __enter__(self):
        self._open()
        return self

    def __exit__ (self, etyp, ev, tb):
        try :
            conn = self._conn
            cur  = self._cursor
            if not conn.autocommit and ev :
                conn.rollback()
                self._logger.warning("transaction rollbacked", exc_info=ev)
            else:
                conn.commit()
        finally:
            self._close()

        return False

    def __lshift__(self, stmt_or_params):
        # """
        # push SQL statement or parameters to dbc.
        # #
        # # dbc << 'SELECT %s'
        # # dbc << (100,)
        #
        # """

        if not stmt_or_params: # when '' or None is pushed, clear sql stmt
            self.__todo_sqlstmt = None
            return self

        if isinstance(stmt_or_params, str): # sql stmt is pushed
            if not self._has_params(stmt_or_params):
                self.__todo_sqlstmt = None
                self.__execute(stmt_or_params)
                return
            else:
                self.__todo_sqlstmt = stmt_or_params
            return self

        # push params
        if not self.__todo_sqlstmt:
            err = 'NO SQL statement to solve the parameters: %r'
            err %= stmt_or_params
            raise ValueError(err)

        if isinstance(stmt_or_params, list):
            self.__execute(self.__todo_sqlstmt, many=stmt_or_params)
            # self.__todo_sqlstmt = None
        elif isinstance(stmt_or_params, (tuple, dict)):
            self.__execute(self.__todo_sqlstmt, params=stmt_or_params)
            # self.__todo_sqlstmt = None
        elif isinstance(stmt_or_params, dobject):
            params = stmt_or_params.export()
            self.__execute(self.__todo_sqlstmt, params=params)
        else:
            err =  'statement should be strm and '
            err += 'parameters should be a tuple, dict and list: %r'
            err %= stmt_or_params
            raise TypeError(err)

        return self

    def __execute(self, sql, params=None, many=None):

        if many is not None:
            self._cursor.executemany(sql, many)
        else:
            self._cursor.execute(sql, params)

        self._iter = None


    def __iter__(self):
        self._iter = self._record_type(self._cursor)
        return self._iter

    def __next__(self):
        _iter = self._iter or self.__iter__()
        try:
            return _iter.__next__()
        except StopIteration:
            return None
        finally:
            self._cur_record_type = None

    @property
    def rowcount(self):
        """
        This read-only attribute specifies the number of rows that the last
        query or the number of affected rows by DML statements.
        """
        self._cursor.rowcount

    @property
    def record_type(self):
        return self._record_type

    @record_type.setter
    def record_type(self, val):
        self._record_type = val

    @abstractmethod
    def nextval(self, seq, batch_cnt=None):
        raise NotImplemented()

    def __dset__(self, item_type):

        dobj_attrs = OrderedDict((attr_name, attr) for attr_name, attr in
                            iter_chain(item_type.__dobject_key__.items(),
                                       item_type.__dobject_att__.items()))

        colnames = []
        selected = []
        for i, d in enumerate(self._cursor.description):
            colname = d[0]
            if colname in dobj_attrs:
                selected.append(i)
                colnames.append(colname)

        print(colnames)
        for record in self._cursor:

            obj = dict((k, v) for k, v in
                                zip(colnames, (record[i] for i in selected)))
            yield item_type(**obj)


def _new_dobject(dobj_cls, attr_pairs):
    """new dobject with attribute (name, value) pairs"""

    instance = dobj_cls()

    attrs  = dobj_cls._dobj_attrs
    values = getattr(instance, '__dobject_attrs')

    for attr_name, attr_val in attr_pairs:
        if attr_name in attrs:
            values[attr_name] = attr_val

    return instance

_default_record_type = record_plainobj


_SQLPillar = pillar_class(BaseSQLBlock, excludes=['__enter__', '__exit__'])
dbc = _SQLPillar(_pillar_history)


def transaction(*d_args, dsn='DEFAULT', autocommit=False):
    """ 对函数方法装配sqlblock，实例会增加sql属性，无异常结束提交事务，有异常则回滚.

    @with_sql
    @with_sql()
    @with_sql(auto_commit=True, dsn='other_db')

    具体使用，必须提供第一个参数
    @with_sql()
    def do_something(self, arg):
        ...
        return ...

    如果该实例已经存在非空的sql属性则会抛出AttributeError
    """

    def _decorator(func):
        def sqlblock_wrapper(*args, **kwargs):

            if dbc._this_object is not None:
                # no allow to reenter a new transaction
                return func(*args, **kwargs)
            else:
                sqlblk = sqlblock(dsn=dsn, autocommit=autocommit)

                def exit_callback(etyp, eval, tb):
                    sqlblk.__exit__(etyp, eval, tb)

                bound_func = _pillar_history.bound(func,
                                            [(dbc, sqlblk)], exit_callback)

                sqlblk.__enter__()
                ret = bound_func(*args, **kwargs)

                if hasattr(ret, '_pillar_history'):
                    raise PillarError('sql block cannot return a pillar object')

                return ret

        functools.update_wrapper(sqlblock_wrapper, func)
        return sqlblock_wrapper

    if len(d_args) == 1 and callable(d_args[0]):
        # no argument decorator
        return _decorator(d_args[0])
    else:
        return _decorator
