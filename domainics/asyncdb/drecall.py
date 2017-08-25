# -*- coding: utf-8 -*-

from itertools import chain as iter_chain

from sqlblock import SQL
from sqlblock.asyncpg import transaction


from ..domobj import DSetBase, DObject
from ..db import dsequence



@transaction._dsn_db
def drecall(obj, present=False, _dsn_db=None):
    """
    Recall the original version of the object from database.

    If present is true, recall the original version of item by item in dset.
    """

    if isinstance(obj, DSetBase):
        if not present:
            return _recall_dset(obj, _dsn_db=_dsn_db)
        else:
            return _recall_dset_by_item(obj, _dsn_db=_dsn_db)

    elif isinstance(obj, DObject):
        return _recall_dobject(obj, _dsn_db=_dsn_db)
    else:
        raise TypeError('Unknown object type: '+ obj.__class__.__name__)


async def _recall_dobject(obj, _dsn_db=None):

    obj_cls = obj.__class__
    col_names = tuple(iter_chain(obj_cls.__dobject_key__, obj.__dobject_att__))

    if hasattr(obj_cls, '__table_name__'):
        table_name = obj_cls.__table_name__
    else:
        table_name = obj.__class__.__name__

    # pk_values = []
    # for val in obj.__dobject_key__:
    #     if isinstance(val, dsequence):
    #         val = val.value
    #     pk_values.append(val)

    pk_pairs = obj.__dobject_key__.as_dict()
    await _select_with_pks(table_name, col_names, pk_pairs,
                            _page=getattr(obj, '_page', None),
                            _dsn_db=_dsn_db)

    try:
        # get the first value
        origin = await _dsn_db.__aiter__().__anext__()
        return obj.__class__(origin)
    except StopAsyncIteration:
        return obj.__class__()

async def _recall_dset_by_item(item_set, _dsn_db=None):
    origin_set = item_set.__class__()
    for item in item_set:
        origin_item = await drecall(item, _dsn_db=_dsn_db)
        if origin_item:
            origin_set._add(origin_item)

    return origin_set


async def _recall_dset(obj, _dsn_db=None):

    item_cls = obj.__dset_item_class__

    if hasattr(item_cls, '__table_name__'):
        table_name = item_cls.__table_name__
    else:
        table_name = item_cls.__name__

    pk_names  = list(item_cls.__dobject_key__)

    col_names = tuple(iter_chain(item_cls.__dobject_key__,
                                        item_cls.__dobject_att__))


    pk_pairs = obj.__dobject_key__.as_dict()
    await _select_with_pks(table_name, col_names, pk_pairs,
                            _page=obj._page, _dsn_db=_dsn_db)

    # empty dset with key
    new_ds = obj.__class__(_dsn_db, **obj.__dobject_key__.as_dict(),
                           _page=obj._page.copy())

    return new_ds

async def _select_with_pks(table_name, col_names, pk_pairs, _page=None, _dsn_db=None):
    if pk_pairs:
        where_part =  SQL("WHERE ")
        where_part += SQL(*(f"{c}={{{c}}}" for c in pk_pairs.keys()), sep=' AND ')
    else:
        where_part =  SQL() # no where condition

    page_part = SQL()
    if _page:
        if _page.sortable:
            page_part += SQL('ORDER BY ')
            page_part += SQL(*(f"{s.name} {'ASC' if s.ascending else 'DESC'}"
                                        for s in _page.sortable), sep=',')

        if _page.limit is not None:
            page_part += SQL('\nLIMIT {_page.limit}')

        if _page.start is not None:
            page_part += SQL('\nOFFSET {_page.start:?}')

    _dsn_db << f"""
    SELECT {','.join(col_names)} FROM {table_name}
    {{where_part}}
    {{page_part}}
    """
    await _dsn_db(**pk_pairs)
