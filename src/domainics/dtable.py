#! /usr/bin/python3
# -*- coding: utf-8 -*-

import datetime
from decimal import Decimal
from collections import OrderedDict, namedtuple
from collections.abc import Iterable

from . import transaction, dbc
from .util import iter_submodules
from .domobj import DObjectMetaClass, dobject, datt, dset

class dtable(dobject):
    pass

class tcol(datt):

    def __init__(self, datatype, len=None, null_ok=True, **kwargs):
        self.len = len

        if issubclass(datatype, dsequence):
            if kwargs.get('default') is not None or not null_ok:
                kwargs['default'] = datatype

        super(tcol, self).__init__(datatype, **kwargs)


def repr_create_table(dobj_cls):

    segments = []
    for name, attr in dobj_cls._dobj_attrs.items():
        s = '  %s %s' % (name, repr_datatype(attr.datatype, attr.len))
        segments.append(s)

    if dobj_cls._dobj_id_names:
        s = '  PRIMARY KEY(%s)' 
        s %=  ','.join(dobj_cls._dobj_id_names)
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
            sql = "COMMENT ON TABLE %s IS '%s';"
            sql %= (table_name, quote(dobj_cls.__doc__))
            yield sql

    for name, attr in dobj_cls._dobj_attrs.items():
        if attr.doc:
            sql = "COMMENT ON COLUMN %s.%s IS '%s';" 
            sql %= (table_name, name, quote(attr.doc))
            yield sql       

def repr_datatype(dtype, length):
    datatype = None
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
            datatype = 'VARCHAR(%d)' % length

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

    elif issubclass(dtype, dsequence):
        if not length:
            datatype = 'INTEGER'
        elif length == 2:
            datatype = 'SMALLINT'
        elif length == 4:
            datatype = 'BIGINT'

    if datatype:
        return datatype

    raise TypeError('unkown type: %s(%r)' % (dtype.__name__, dopts)) 


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

def repr_drop_table(dobj_cls):
    s = "DROP TABLE IF EXISTS %s;" 
    s %= dobj_cls.__name__
    yield s

def repr_drop_sequence(dobj_cls):
    s = "DROP SEQUENCE IF EXISTS %s;" 
    s %= dobj_cls.__name__
    yield s



class dsequence:

    def __init__(self, value=None):
        if value is None or isinstance(value, int):
            self.__value = value
        elif isinstance(value, str):
            self.__value = int(value)
        else:
            err = 'dsequence should be integer, not %s'
            err %= value.__class__.__name__
            raise TypeError(err)

    @property    
    def value(self):
        if self.__value is None:
            err = 'The sequence number %s is not allocated'
            err %= self.__class__.__name__
            raise ValueError(err)

        return self.__value

    @value.setter
    def value(self, newval):
        if isinstance(newval, int):
            self.__value = newval
        else:
            err = 'The sequence value should be int, not %s'
            err %= newval.__class__.__name__
            raise TypeError(err)

    def __bool__(self):
        return self.__value is not None

    def __int__(self):
        return self.value

    def __hash__(self):
        return super(dsequence, self).__hash__()

    def __repr__(self):
        if self.__value is not None:
            return repr(self.__value)
        else:
            return self.__class__.__name__ + '(' + ')'




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
        for module in iter_submodules(root_module):
            for objname in dir(module):
                obj = getattr(module, objname)

                if not isinstance(obj, type):
                    continue

                if not issubclass(obj, (dtable, dsequence)) :  
                    continue

                if obj is dtable or obj is dsequence:
                    continue

                self.schema_objs.append(obj)

    def create(self):
        stmts = ['\n']
        for db_cls in self.schema_objs:
            if issubclass(db_cls, dtable):
                for stmt in repr_create_table(db_cls):
                    stmts.append(stmt)
            elif issubclass(db_cls, dsequence):
                for stmt in repr_create_sequence(db_cls):
                    stmts.append(stmt)
        
        stmts = '\n'.join(stmts)
        self.__tfunc(stmts)

    def drop(self):
        stmts = ['\n']
        for db_cls in self.schema_objs:
            if issubclass(db_cls, dtable):
                for stmt in repr_drop_table(db_cls):
                    stmts.append(stmt)
            elif issubclass(db_cls, dsequence):
                for stmt in repr_drop_sequence(db_cls):
                    stmts.append(stmt)

        stmts = '\n'.join(stmts)
        self.__tfunc(stmts)


_dtdelta = namedtuple('DTDelta', ['pkeys', 'fields', 'pkvals', 'values'])


def _dtable_diff(current, past=None):
    """diff dtable object, return the delta information.

    The delta information is a tuple, the data is added, changged and removed. 
    """
    if current is None and past is None:
        return

    if current is not None and not isinstance(current, dset):
        if not isinstance(current, dobject):
            err = 'The current object should be dobject or dset type: %s'
            err %= current.__class__.__name__
            raise TypeError(err)

        dos = dset(item_type=current.__class__)
        dos.append(current)
        current = dos

    if past is not None and not isinstance(past, dset):
        if not isinstance(current, dobject):
            err = 'The past object should be dobject or dset type: %s'
            err %= past.__class__.__name__
            raise TypeError(err)

        dos = dset(item_type=past.__class__)
        dos.append(past)
        past = dos

    if current is None:
        current = dset(item_type=past.item_type)

    if past is None:
        past = dset(item_type=current.item_type)

    inslst = [] # the objects to be inserted
    dellst = [] # the objid to be deleted
    chglst = [] # the current and past objects to be modified


    attrnames = tuple(current.item_type._dobj_attrs.keys())
    for curr_obj in current:
        if curr_obj not in past:
            inslst.append(curr_obj)
            continue

        past_obj = past[curr_obj]

        modified = OrderedDict()
        for attrname in attrnames:
            if not hasattr(past_obj, attrname):
                continue

            newval = getattr(curr_obj, attrname)
            oldval = getattr(past_obj, attrname)
            if newval == oldval:
                continue

            modified[attrname] = (newval, oldval)
            # modified_attrs.add(attrname)

        if modified:
            chglst.append((curr_obj._dobj_id, modified))

    for past_obj in past:
        if past_obj not in current:
            dellst.append(past_obj)

    # inserted data tuple
    item_type = current.item_type
    pkeys = tuple(item_type._dobj_id_names)
    fields  = tuple(f for f in item_type._dobj_attrs if f not in pkeys)
    pkvals, values = [], []
    for obj in inslst:
        pkvals.append(tuple(getattr(obj, f) for f in pkeys))
        values.append(tuple(getattr(obj, f) for f in fields))
    
    dt_ins = _dtdelta(pkeys, fields, pkvals, values)

    # deleted data tuple
    item_type = past.item_type
    pkeys  = tuple(item_type._dobj_id_names)
    pkvals = []
    for obj in dellst:
        pkvals.append(tuple(getattr(obj, f) for f in pkeys))
    
    dt_del = _dtdelta(pkeys, None, pkvals, None)

    # modified data tuple
    item_type = past.item_type
    pkeys  = tuple(item_type._dobj_id_names)
    fields = tuple(f for f in item_type._dobj_attrs if f not in pkeys)

    pkvals = []
    values = []
    for objid, modified in chglst:
        pkvals.append(tuple(getattr(objid, f) for f in pkeys))
        values.append(modified)

    dt_chg = _dtdelta(pkeys, fields, pkvals, values)

    return dt_ins, dt_chg, dt_del

def pq_dtable_merge(current, past):

    dins, dchg, ddel = _dtable_diff(current, past)
    table_name = current.item_type.__name__

    seq_attrs = {}
    for n, f in current.item_type._dobj_attrs.items():
        if issubclass(f.datatype, dsequence):
            seq_attrs[n] = f


    if dins.values:
        cols = dins.pkeys + dins.fields
        values = [k + v for k, v in zip(dins.pkvals, dins.values)]

        # If there are new sequence objects, 
        # get next values of them in a batch
        if seq_attrs:
            seq_cols = [] # (col_idx, col_name, [seq_value])
            for i, colname in enumerate(cols):
                if colname in seq_attrs:
                    seq_cols.append((i, colname, []))

            for record in values:
                for seq_col in seq_cols:
                    seq_val = record[seq_col[0]]
                    if seq_val is not None:
                        seq_col[2].append(seq_val)

            for colidx, colname, seqvals in seq_cols:
                if not seqvals:
                    continue

                # nextval of sequence
                seqname = seq_attrs[colname].datatype.__name__
                newvals = dbc.nextval(seqname, batch_cnt=len(seqvals))

                for seq, value in zip(seqvals, newvals):
                    seq.value = value
        
        dbc << """
        INSERT INTO {table} ({cols}) 
        VALUES ({vals});
        """.format(table=table_name, 
                   cols=', '.join(cols), 
                   vals=', '.join(['%s'] * len(cols)))
        
        dbc << values

    if dchg.values:

        if seq_attrs:
            # for attrname in dchg.values.items():

            seq_cols = {}
            for record in dchg.values:
                for colname in record:
                    if colname in seq_attrs:
                        try:
                            seqvals = seq_cols[colname]
                        except KeyError: 
                            seq_cols[colname] = seqvals = []

                        seqvals.append(record[colname][0])

            for colname, seqvals in seq_cols.items():
                if not seqvals:
                    continue

                # nextval of sequence
                seqname = seq_attrs[colname].datatype.__name__
                newvals = dbc.nextval(seqname, batch_cnt=len(seqvals))

                for seq, value in zip(seqvals, newvals):
                    seq.value = value

        # generate update statment in a modified field group
        groups = {}
        for i, modified in enumerate(dchg.values): # group with attr name
            grpid = tuple(modified.keys())
            try:
                chgidxs = groups[grpid]
            except KeyError:
                groups[grpid] = chgidxs = []

            chgidxs.append(i)

        pkcond = ' AND '.join(['{pk}=%s'.format(pk=pk) for pk in dchg.pkeys])
        for grpid, chgidxs in groups.items():
            asgn_expr = ', '.join(['%s=%%s' % name for name in grpid])

            dbc << """
            UPDATE {table} SET
            {asgn} 
            WHERE {pkcond}
            """.format(table=table_name, asgn=asgn_expr, pkcond=pkcond)

            for i in chgidxs:
                values = tuple(dchg.values[i][k][0] for k in grpid)
                pkvals = dchg.pkvals[i]
                dbc << values + pkvals

    if ddel.pkvals:
        pkcond = ' AND '.join(['{pk}=%s'.format(pk=pk) for pk in ddel.pkeys])

        dbc << """
        DELETE FROM {table} WHERE {pkcond};
        """.format(table=table_name, pkcond=pkcond)
        
        dbc << [k for k in ddel.pkvals]

def dmerge(current, past=None):
    if dbc.dbtype == 'postgres':
        pq_dtable_merge(current, past)
