# -*- coding: utf-8 -*-

from itertools import chain as iter_chain

from .sqlblock import dbc
from ..domobj import DSetBase, DObject


def drecall(obj):
    """
    Recall the original version of the object from database.
    """

    if isinstance(obj, DSetBase):
        return _recall_dset(obj)

    elif isinstance(obj, DObject):
        return _recall_dobject(obj)
    else:
        raise TypeError('Unknown object type: '+ obj.__class__.__name__)


def _recall_dobject(obj):

    obj_cls = obj.__class__
    col_names = tuple(iter_chain(obj_cls.__dobject_key__, obj.__dobject_att__))

    sql = "SELECT " + ','.join(col_names) + " FROM "
    sql += obj.__class__.__name__ + " WHERE "
    sql += ' AND '.join( pk_colname + "=%s"
                            for pk_colname in obj_cls.__dobject_key__)

    pk_values = []
    for val in obj.__dobject_key__:
        pk_values.append(val)

    dbc << sql << tuple(pk_values)
    origin = next(dbc)
    if origin is not None:
        return obj.__class__(origin)
    else:
        return obj.__class__()

def _recall_dset(obj):

    item_cls = obj.__dset_item_class__
    tbl_name = item_cls.__name__


    pk_names = []

    col_names = tuple(iter_chain(item_cls.__dobject_key__,
                                        item_cls.__dobject_att__))


    sql = "SELECT " + ', '.join(col_names) + " FROM " + tbl_name

    if obj.__dobject_key__:
        sql += '\nWHERE '
        sql += ' AND '.join(n + "=%s" for n in obj.__class__.__dobject_key__)

    pk_values = []
    for val in obj.__dobject_key__:
        pk_values.append(val)

    dbc << sql << tuple(pk_values)

    # empty dset with key
    new_ds = obj.__class__(dbc, **obj.__dobject_key__.as_dict())

    return new_ds
