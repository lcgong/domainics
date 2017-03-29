# -*- coding: utf-8 -*-

import logging

import sys
import functools
from string import Formatter
from collections  import OrderedDict
from collections.abc  import Iterable
from collections  import namedtuple  as _namedtuple
from itertools import chain as iter_chain
from abc import abstractmethod
from inspect import isgenerator

from ..util     import nameddict   as _nameddict
from ..pillar   import _pillar_history, pillar_class, PillarError, History
from ..domobj   import dobject

from ..pillar   import P


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

@transaction(alias='sql_a')

"""


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

class SQLSegmentList(list):
    pass

_formatter = Formatter()
class SQLText:
    __tuple__ = ('_segments',)

    def __init__(self, sep=''):
        self._segments = SQLSegmentList()
        self._sep = sep

    def clear(self):
        self._segments.clear()

    def __bool__(self):
        return bool(self._segments)

    def __call__(self, *sql_texts, sep=None):
        self._join(sql_texts, sep, sys._getframe(1))
        return self

    def __lshift__(self, sql_text):
        self._join((sql_text,), None, sys._getframe(1))
        return self

    def _join(self, sql_texts, sep, frame):
        if sep is None:
            sep = self._sep

        sql_text_iter = iter(sql_texts)
        sql_text = next(sql_text_iter, None)
        if sql_text:
            if isgenerator(sql_text):
                self._join(sql_text, sep, sql_text.gi_frame)
            else:
                segments = self._interpolate(sql_text, frame)
                if segments:
                    if self._segments:
                        self._segments.append(SQLSegment(sep, frame))

                    self._segments += segments

        for sql_text in sql_text_iter:
            if isgenerator(sql_text):
                self._segments.append(SQLSegment(sep, frame))
                self._join(sql_text, sep, sql_text.gi_frame)
            else:
                self._segments.append(SQLSegment(sep, frame))
                self._segments += self._interpolate(sql_text, frame)

    def _interpolate(self, sqltext, frame):
        segments = []
        for text, field_name, format_spec, conversion in _formatter.parse(sqltext):
            segments.append(SQLSegment(text, frame))
            if field_name:
                val = eval(field_name, frame.f_globals, frame.f_locals)
                if isinstance(val, SQLText):
                    segments += val._segments
                else:
                    seg = SQLSegment('%s', frame)
                    seg.values.append(val)
                    segments.append(seg)

        return segments

    def get_statment(self):
        sql_text = ''
        sql_vals = []
        for seg in self._segments:
            sql_text += seg.text
            sql_vals += seg.values

        return sql_text, sql_vals

class SQLSegment:

    def __init__(self, text, frame):
        self.offset = (0, 0) # lineno, charpos at line
        self.text = text
        self.values = []
        self.frame = frame # frame.f_lineno frame.f_code.co_filename

        # compute the offset of this segment
        lines = text.splitlines()
        if lines:
            offset_lineno = len(lines) - 1
            offset_charpos = len(lines[offset_lineno])

            self.offset = (offset_lineno, offset_charpos)


    def __repr__(self):
        return f'SQLSegment(text=\"{self.text}\")'



def sqltext(*sql_texts, sep=''):
    sqltext = SQLText(sep=sep)
    sqltext._join(sql_texts, sep, sys._getframe(1))
    return sqltext

class BaseSQLBlock:
    def __init__(self, dbms, dsn='DEFAULT', autocommit=False, record_type=None):

        self._record_type = record_type or _default_record_type

        self.dbms = dbms
        self.dsn = dsn
        self.autocommit = autocommit

        # self._cur_record_type = None
        self._iter = None
        self._sql_builder = SQLText()

        self._logger = logging.getLogger(__name__)

    def __enter__(self):
        self._open()
        return self

    def __exit__ (self, etyp, ev, tb):
        try :
            self._push()

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

    def __call__(self, *sql_texts, sep=''):
        if self._iter:
            self._iter.close()
            self._iter = None

        self._sql_builder._join(sql_texts, sep, sys._getframe(1))
        return self

    def __lshift__(self, sql_text):
        if self._iter:
            self._iter.close()
            self._iter = None

        self._sql_builder._join([sql_text], '', sys._getframe(1))
        return self

    def _push(self):
        sql_stmt, sql_vals = self._sql_builder.get_statment()
        if sql_stmt:
            self._cursor.execute(sql_stmt, sql_vals)
        self._sql_builder.clear()

    def __iter__(self):
        self._push()

        self._iter = self._record_type(self._cursor)
        return self._iter

    def __next__(self):
        _iter = self._iter or self.__iter__()
        try:
            return _iter.__next__()
        except StopIteration:
            self._iter = None
            return None

    @property
    def next(self):
        return next(self)

    @property
    def rowcount(self):
        """
        This read-only attribute specifies the number of rows that the last
        query or the number of affected rows by DML statements.
        """
        return self._cursor.rowcount

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

from collections import namedtuple

_DBSource = namedtuple('DBSource', ['dsn', 'autocommit'])

pillar_has_name = P.__pillar__.has_name
pillar_confine = P.__pillar__.confine

class TransactionDecorator:

    def __init__(self, alias='sql'):
        self.alias = alias

    def __getattr__(self, alias): # new decorator for a new alias
        transaction = TransactionDecorator(alias)
        return transaction

    def __call__(self, *d_args, dsn='DEFAULT', autocommit=False, alias=None):
        if alias is None:
            alias = self.alias

        def _decorator(func):
            __sqlblock_sources__ = getattr(func, '__sqlblock_sources__', None)
            if __sqlblock_sources__ is None:
                __sqlblock_sources__ = {}
                setattr(func, '__sqlblock_sources__', __sqlblock_sources__)

            __sqlblock_sources__[alias] = _DBSource(dsn, autocommit)

            def sqlblock_wrapper(*args, **kwargs):

                # if pillar_has_name(alias): # forbid to reenter a transaction
                #     return func(*args, **kwargs)

                # else:
                blocks = {}
                for alias, src in __sqlblock_sources__.items():
                    if pillar_has_name(alias):
                        # forbid to reenter a sqlblock of transaction
                        continue
                        # return func(*args, **kwargs)
                    blocks[alias] = sqlblock(dsn=src.dsn, autocommit=src.autocommit)

                if blocks:
                    def callback(etyp, eval, tb):
                        for sqlblk in blocks.values():
                            sqlblk.__exit__(etyp, eval, tb)

                    bound_func = pillar_confine(func, **blocks, exit_callback=callback)

                    # bound_func = _pillar_history.bound(func,
                    #                             [(dbc, sqlblk)], exit_callback)

                    for sqlblk in blocks.values():
                        sqlblk.__enter__()

                    return bound_func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)
                #     bound_func = func
                #
                # return func(*args, **kwargs)

            functools.update_wrapper(sqlblock_wrapper, func)
            return sqlblock_wrapper

        if len(d_args) == 1 and callable(d_args[0]):
            # no argument decorator
            return _decorator(d_args[0])
        else:
            return _decorator


transaction = TransactionDecorator()
