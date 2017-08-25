# -*- coding: utf-8 -*-

import pytest

from domainics.domobj import dobject, datt, dset
from domainics.asyncdb.dtable import dtable, dsequence, array, json_object
from domainics.asyncdb.dmerge import dmerge

from sqlblock.asyncpg import transaction
from sqlblock import SQL

class t_a(dtable):
    a = datt(int)
    b = datt(array(str, dimensions=1))

    __dobject_key__ = [a]

@pytest.mark.asyncio
@transaction.db
async def test_array(db, module_dtables):

    ASet = dset(t_a)
    s1 = ASet()

    r1 = t_a(a=1)
    s1 += [r1]
    s1[0].b = ['a', 'b']
    assert s1[0].b == ['a', 'b']

    await dmerge(s1)

    await (db << "SELECT * FROM t_a ORDER BY a")

    s2 = ASet(db)
    assert s2[0].b == ['a', 'b']

    print(s2)
