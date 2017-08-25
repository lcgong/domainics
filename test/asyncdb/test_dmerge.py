# -*- coding: utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

import pytest
import inspect
from datetime import datetime, date
from decimal import Decimal

from domainics.domobj import dobject, datt, dset
from sqlblock.asyncpg import transaction
from sqlblock import SQL

from domainics.domobj import dobject, datt, dset
from domainics.asyncdb.dtable import dtable, dsequence
from domainics.asyncdb.schema import DBSchema

from domainics.asyncdb.dmerge import dmerge


class t_a(dtable):
    a = datt(int)
    b = datt(int)
    c = datt(int, doc="should be constrained")
    d = datt(int)
    e = datt(int, doc="should be constrained")
    f = datt(int)

    __dobject_key__ = [a, b]

@pytest.mark.asyncio
@transaction.db
async def test_dmerge(db, module_dtables):
    # schema = DBSchema()
    # schema.add_module(inspect.getmodule(t_a))
    # await schema.drop()
    # await schema.create()

    # orginal data
    ASet = dset(t_a)

    ds = ASet([t_a(a=101, b=102, c=103, d=104, e=105),
               t_a(a=201, b=202, c=203, d=204, e=205)])

    t_a1 = t_a._re(_ignore=['c', 'e'])
    A1Set = dset(t_a1)

    ds1 = A1Set(ds)

    await dmerge(ds1)

    # return

    db << "SELECT * FROM t_a"
    await db

    ds0 = A1Set(db) # original
    ds1 = A1Set(ds0) # to be modified

    ds1 += [t_a1(a=301, b=302, d=304, f=306)] # insert a new item

    ds1[t_a(a=101, b=102)].f = 5555 # update the first item

    del ds1[t_a(a=201, b=202)] # delete the 2nd item

    print('ORIGINAL:', ds0)
    print('MODIFIED:', ds1)

    await dmerge(ds1, ds0)

    SQL("SELECT * FROM t_a ORDER BY a,b") >> db
    ds2 = A1Set(await db)
    print("SELECTED:", ds2)

    assert ds1[0] == ds2[0] and ds1[0].f == ds2[0].f
    assert ds1[1] == ds2[1]


# import inspect
# print(inspect.signature(test_dmerge).parameters)
