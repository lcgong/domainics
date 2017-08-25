# -*- coding: utf-8 -*-

import pytest
import inspect

from domainics.domobj import DPage, DSet, dobject, datt, dset
from domainics.asyncdb.dtable import dtable, dsequence
from domainics.asyncdb.schema import DBSchema
from domainics.asyncdb.dmerge import dmerge
from domainics.asyncdb.drecall import drecall

from sqlblock.asyncpg import transaction
from sqlblock import SQL

class test_a(dtable):
    sn = datt(int)
    line = datt(int)
    name = datt(str)

    __dobject_key__ = [sn, line]

@pytest.mark.asyncio
@transaction.db
async def test_case2(db, module_dtables):

    ASet = dset(test_a, _key=dict(sn=datt(int)))
    ds1 = ASet(sn=101)
    for i in range(10):
        ds1 += [test_a(line=i, name='L%03d' % i)]
    await dmerge(ds1)

    ds1 = ASet(sn=202)
    for i in range(10):
        ds1 += [test_a(line=i, name='L%03d' % i)]
    await dmerge(ds1)

    print('----------------')
    await (db << "SELECT * FROM test_a")
    async for r in db:
        print(r)
    print('+++++++++++++++')

    page = DPage(start=2, limit=3, sortable='+sn,-line')
    ds1 = ASet(sn=101, _page=page)
    ds2 = await drecall(ds1)

    print('ds: ', ds2)

    assert len(ds2) == 3
    assert ds2[test_a(line=7)].line == 7
    assert not ds2[test_a(line=4)]
    assert not ds2[test_a(line=8)]
