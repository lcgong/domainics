# -*- coding: utf-8 -*-

import pytest
import inspect

# from domainics.tornice import rest, route_base
# from domainics.tornice.client import rest_client, restcli

from domainics.domobj import DPage, DSet, dobject, datt, dset
from domainics.asyncdb.dtable import dtable, dsequence
from domainics.asyncdb.sqlblock import transaction
from domainics.asyncdb.schema import DBSchema
from domainics.sqltext import SQL
from domainics.asyncdb.dmerge import dmerge
from domainics.asyncdb.drecall import drecall


# set_dsn(sys='postgres', database="ci_test", host='localhost', user='dbo')
#
# def setup_function(function):
#     print()
#     module = inspect.getmodule(function)
#
#     schema = DBSchema()
#     schema.add_module(module)
#     schema.drop()
#     schema.create()


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
    await db.execute("SELECT * FROM test_a")
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