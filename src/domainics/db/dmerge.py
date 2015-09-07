# -*- coding: utf-8 -*-

import datetime
from decimal import Decimal
from collections import OrderedDict, namedtuple
from collections.abc import Iterable



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
        
        item_val = []
        for fieldname in fields:
            field = item_type._dobj_attrs[fieldname]
            field_value = getattr(obj, fieldname)

            if issubclass(field.datatype, json_object):
                field_value = json.dumps(field_value)
            
            item_val.append(field_value)

        values.append(tuple(item_val))
    
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
