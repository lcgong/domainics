# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger(__name__)

import pytest
import inspect
from datetime import datetime, date
from decimal import Decimal

from sqlblock.asyncpg import transaction
from sqlblock import SQL
from domainics.domobj import dobject, datt, dset
from domainics.domobj import dobject, datt, dset
from domainics.asyncdb.dtable import dtable, dsequence

from domainics.asyncdb.dmerge import dmerge
from domainics.asyncdb.drecall import drecall

class t_b(dtable):
    a = datt(int)
    b = datt(int)
    c = datt(int)
    d = datt(int)

    __dobject_key__ = [a, b]


@pytest.mark.asyncio
@transaction.db
async def test_recall1(db, module_dtables):

    ASet = dset(t_b, _key=dict(a=datt(int))) # define a key of dset

    ds1 = ASet(a=11)
    ds1 += [t_b(b=12, c=13, d= 14), t_b(b=22, c=23, d=24)]
    await dmerge(ds1)

    r1 = await drecall(t_b(a=11, b=12))
    assert r1.a == 11 and r1.b == 12 and r1.c == 13

    r1 = await drecall(t_b(a=99, b=12)) # no found and return empty dobject
    assert r1 is not None and not bool(r1)

@pytest.mark.asyncio
@transaction.db
async def test_recall2(db, module_dtables):

    ASet = dset(t_b, _key=dict(a=datt(int))) # define a key of dset

    ds1 = ASet(a=1)
    ds1 += [t_b(b=12, c=13, d= 14), t_b(b=22, c=23, d=24)]
    await dmerge(ds1)

    db << 'SELECT 1 as sn FROM t_b'
    await db

    ds2 = ASet(a=2)
    print(333, ds2)
    ds2 += [t_b(b=32, c=33, d= 34), t_b(b=42, c=43, d=44)]
    await dmerge(ds2)

    ds1 = await drecall(ASet(a=1))
    assert len(ds1) == 2

    ds1 += [t_b(b=12, c=113, d=114), t_b(b=62, c=63, d=64)]
    del ds1[t_b(b=22)]
    with pytest.raises(KeyError):
        del ds1[t_b(b=55)]

    ds0 = await drecall(ASet(a=1))

    print("O: ", ds0)
    print("N: ", ds1)

    await dmerge(ds1, ds0)

    ds2 = await drecall(ASet(a=1))
    assert ds2[0].c == ds1[0].c
    assert ds2[1].c == ds1[1].c

@pytest.mark.asyncio
@transaction.db
async def test_recall_without_dset_key(db, module_dtables):
    ASet = dset(t_b) # define a key of dset

    ds1 = ASet()
    ds1 += [t_b(a=1, b=12, c=13, d= 14), t_b(a=1, b=22, c=23, d=24)]
    ds1 += [t_b(a=1, b=32, c=33, d= 34), t_b(a=1, b=42, c=43, d=44)]
    await dmerge(ds1)

    ds2 = await drecall(ASet())
    assert len(ds2) == 4
    print(ds2)
