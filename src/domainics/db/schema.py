# -*- coding: utf-8 -*-

import datetime
from decimal import Decimal
from collections import OrderedDict, namedtuple
from collections.abc import Iterable

# from .. import json
from .sqlblock import transaction, dbc
from .dtable import dtable, dsequence, json_object, DBArray
from ..util     import iter_submodules


from ..domobj import dobject, dset, datt

import textwrap

from itertools import chain as iter_chain

def repr_create_table(dobj_cls):

    attrs = OrderedDict((attr_name, attr) for attr_name, attr in
                        iter_chain(dobj_cls.__dobject_key__.items(),
                                   dobj_cls.__dobject_att__.items()))

    segments = []
    for name, attr in attrs.items():
        s = '  %s %s' % (name, repr_datatype(attr.type, attr.len))
        segments.append(s)

    if dobj_cls.__dobject_key__:
        s = '  PRIMARY KEY(%s)'
        s %=  ','.join(dobj_cls.__dobject_key__.keys())
        segments.append(s)

    if hasattr(dobj_cls, '__tablename__'):
        table_name = dobj_cls.__tablename__
    else:
        table_name = dobj_cls.__name__

    sql = ''.join([
        'CREATE TABLE IF NOT EXISTS ', table_name,
        ' (\n', ',\n'.join(segments), '\n);'
        ])
    yield sql

    quote = lambda s : s.replace("'", "''")


    if hasattr(dobj_cls, '__doc__') and dobj_cls.__doc__:
        if dobj_cls.__doc__:
            doc = textwrap.dedent(dobj_cls.__doc__)
            sql = "COMMENT ON TABLE %s IS '%s';"
            sql %= (table_name, quote(doc))
            yield sql

    for name, attr in attrs.items():
        if attr.doc:
            doc = textwrap.dedent(attr.doc)
            sql = "COMMENT ON COLUMN %s.%s IS '%s';"
            sql %= (table_name, name, quote(doc))
            yield sql

def repr_datatype(dtype, length=None):
    datatype = None
    assert dtype is not None

    if dtype == int:
        if not length:
            datatype = 'INTEGER'
        elif length == 2:
            datatype = 'SMALLINT'
        elif length == 4:
            datatype = 'BIGINT'

    elif dtype == str:

        if not length:
            datatype = 'TEXT'
        else:
            # print('%r, %s' % (length, length.__class__))
            datatype = 'VARCHAR(%d)' % length

    elif dtype == float:
        if length is not None:
            datatype = 'FLOAT(%d)' % length
        else:
            datatype = 'FLOAT' # 8 bytes

    elif dtype == bool:
        datatype = 'BOOLEAN'

    elif dtype == Decimal:
        if not length:
            datatype = 'NUMERIC'
        elif isinstance(length, int):
            datatype = 'NUMERIC(%d)' % length
        elif isinstance(length, Iterable):
            length = [repr(i) for i in length]
            datatype = 'NUMERIC(%s)' % ','.join(list(length))

    elif dtype == datetime.date:
        datatype = 'DATE'

    elif issubclass(dtype, datetime.datetime):
        datatype = 'TIMESTAMP'

    elif issubclass(dtype, dsequence):
        if not length:
            datatype = 'INTEGER'
        elif length == 2:
            datatype = 'SMALLINT'
        elif length == 4:
            datatype = 'BIGINT'

    elif issubclass(dtype, json_object):
        datatype = 'JSONB'

    elif issubclass(dtype, DBArray):
        datatype = repr_datatype(dtype.item_type) + '[]' * dtype.dimensions

    if datatype:
        return datatype

    raise TypeError('unkown type: %s' % dtype.__name__)


def repr_create_sequence(dobj_cls):
    start = dobj_cls.start if dobj_cls.start is not None else 1
    step  = dobj_cls.step  if dobj_cls.step  is not None else 1
    s = [
        'CREATE SEQUENCE ', dobj_cls.__name__,
        ' INCREMENT BY ', repr(step),
        ' START WITH ', repr(start), ';'
        ]

    s = ''.join(s)

    yield s


    quote = lambda s : s.replace("'", "''")
    if hasattr(dobj_cls, '__doc__') and dobj_cls.__doc__:
        if dobj_cls.__doc__:

            doc = textwrap.dedent(dobj_cls.__doc__)
            sql = "COMMENT ON SEQUENCE %s IS '%s';"
            sql %= (dobj_cls.__name__, quote(doc))
            yield sql

def repr_drop_table(dobj_cls):
    s = "DROP TABLE IF EXISTS %s;"
    s %= dobj_cls.__name__
    yield s

def repr_drop_sequence(dobj_cls):
    s = "DROP SEQUENCE IF EXISTS %s;"
    s %= dobj_cls.__name__
    yield s



class DBSchema:

    def __init__(self, dsn='DEFAULT'):
        self.dsn = dsn
        self.schema_objs = []

        def execsql(*stmts):
            for stmt in stmts:
                dbc << stmt

        self.__tfunc = transaction(dsn)(execsql)

    def add_module(self, root_module):
        """ """

        table_names = OrderedDict()
        seen   = set()

        for module in iter_submodules(root_module):
            for objname in dir(module):
                obj = getattr(module, objname)

                if not isinstance(obj, type):
                    continue

                if not issubclass(obj, (dtable, dsequence)) :
                    continue

                if obj is dtable or obj is dsequence:
                    continue

                qual_name = obj.__module__ + '.' + obj.__name__

                if qual_name in seen:
                    continue

                seen.add(qual_name)

                if obj.__name__ in table_names:
                    errmsg = "Database relation '%s' has already been defined in %s"
                    errmsg %= (obj.__name__, table_names[obj.__name__])
                    raise TypeError(errmsg)

                table_names[obj.__name__] = qual_name

                self.schema_objs.append(obj)

    def create(self):
        if not self.schema_objs:
            return

        seen = set()

        stmts = ['\n']
        for db_cls in self.schema_objs:
            cls_name = db_cls.__module__ + '.' + db_cls.__name__

            if cls_name in seen:
                continue

            try :
                if issubclass(db_cls, dtable):
                    seen.add(cls_name)
                    for stmt in repr_create_table(db_cls):
                        stmts.append(stmt)
                elif issubclass(db_cls, dsequence):
                    seen.add(cls_name)
                    for stmt in repr_create_sequence(db_cls):
                        stmts.append(stmt)
            except Exception as ex:
                # print(ex)
                errmsg = 'caught exception while scheming %s (%r)'
                errmsg %= (db_cls.__name__, ex)
                # raise TypeError(errmsg) from ex
                raise

        stmts = '\n'.join(stmts)
        self.__tfunc(stmts)

    def drop(self):
        if not self.schema_objs:
            return

        seen = set()

        stmts = ['\n']
        for db_cls in self.schema_objs:
            if db_cls in seen:
                continue

            if issubclass(db_cls, dtable):
                seen.add(db_cls)
                for stmt in repr_drop_table(db_cls):
                    stmts.append(stmt)
            elif issubclass(db_cls, dsequence):
                seen.add(db_cls)
                for stmt in repr_drop_sequence(db_cls):
                    stmts.append(stmt)

        stmts = '\n'.join(stmts)
        self.__tfunc(stmts)
