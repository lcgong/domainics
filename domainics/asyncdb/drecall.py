# -*- coding: utf-8 -*-

from itertools import chain as iter_chain

from .sqlblock import dbc
from ..domobj import DSetBase, DObject
from ..db import dsequence

def drecall(obj, present=False):
    """
    Recall the original version of the object from database.

    If present is true, recall the original version of item by item in dset.
    """

    if isinstance(obj, DSetBase):
        if not present:
            return _recall_dset(obj)
        else:
            return _recall_dset_by_item(obj)

    elif isinstance(obj, DObject):
        return _recall_dobject(obj)
    else:
        raise TypeError('Unknown object type: '+ obj.__class__.__name__)


def _recall_dobject(obj):

    obj_cls = obj.__class__
    col_names = tuple(iter_chain(obj_cls.__dobject_key__, obj.__dobject_att__))

    if hasattr(obj_cls, '__table_name__'):
        table_name = obj_cls.__table_name__
    else:
        table_name = obj.__class__.__name__

    sql = "SELECT " + ','.join(col_names) + " FROM "
    sql += table_name + " WHERE "
    sql += ' AND '.join( pk_colname + "=%s"
                            for pk_colname in obj_cls.__dobject_key__)

    pk_values = []
    for val in obj.__dobject_key__:
        if isinstance(val, dsequence):
            val = val.value
        pk_values.append(val)


    dbc << sql << tuple(pk_values)
    origin = next(dbc)
    if origin is not None:
        return obj.__class__(origin)
    else:
        return obj.__class__()

def _recall_dset_by_item(item_set):
    origin_set = item_set.__class__()
    for item in item_set:
        origin_item = drecall(item)
        if origin_item:
            origin_set._add(origin_item)

    return origin_set

def _recall_dset(obj):

    item_cls = obj.__dset_item_class__

    if hasattr(item_cls, '__table_name__'):
        table_name = item_cls.__table_name__
    else:
        table_name = item_cls.__name__

    pk_names = []

    col_names = tuple(iter_chain(item_cls.__dobject_key__,
                                        item_cls.__dobject_att__))

    pk_values = []
    for val in obj.__dobject_key__:
        pk_values.append(val)

    sql = "\nSELECT " + ', '.join(col_names) + " FROM " + table_name

    if pk_values:
        sql += '\nWHERE '
        sql += ' AND '.join(n + "=%s" for n in obj.__class__.__dobject_key__)

    sql += _make_page_sql(obj._page)

    dbc << sql
    if pk_values:
        dbc << tuple(pk_values)

    # empty dset with key
    new_ds = obj.__class__(dbc, **obj.__dobject_key__.as_dict(),
                           _page=obj._page.copy())

    return new_ds

def _make_page_sql(page) :
    if not page:
        return ''


    sql = ''
    if page.sortable:
        sortables = []
        for s in page.sortable:
            asc = 'ASC' if s.ascending else 'DESC'
            sortables.append(s.name + ' ' + asc)

        sql += '\nORDER BY ' + ', '.join(sortables)

    if page.limit is not None:
        sql += '\nLIMIT %d' % page.limit

    if page.start is not None:
        sql += '\nOFFSET %d' % page.start

    return sql
