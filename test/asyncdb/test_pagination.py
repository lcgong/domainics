# -*- coding: utf-8 -*-

import pytest
import inspect
import decimal
from urllib.parse import urljoin
from datetime import date, datetime

from domainics.domobj import DPage, DSet, dobject, datt, dset
from domainics.db import dmerge, drecall, set_dsn, transaction, dtable
from domainics.db import DBSchema
from domainics.tornice import rest, route_base
from domainics.tornice.client import rest_client, restcli

#---------------------------------------------------------------------------

set_dsn(sys='postgres', database="ci_test", host='localhost', user='dbo')

def setup_function(function):
    print()
    module = inspect.getmodule(function)

    schema = DBSchema()
    schema.add_module(module)
    schema.drop()
    schema.create()


class test_a(dtable):
    sn = datt(int)
    line = datt(int)
    name = datt(str)

    __dobject_key__ = [sn, line]

@transaction
def test_case2():

    ASet = dset(test_a, _key=dict(sn=datt(int)))
    ds1 = ASet(sn=101)
    for i in range(10):
        ds1 += [test_a(line=i, name='L%03d' % i)]

    ds1 = ASet(sn=202)
    for i in range(10):
        ds1 += [test_a(line=i, name='L%03d' % i)]

    dmerge(ds1)

    page = DPage(start=2, limit=3, sortable='+sn,-line')
    ds1 = ASet(sn=101, _page=page)
    ds2 = drecall(ds1)

    print('ds: ', ds2)

    assert len(ds2) == 3
    assert ds2[test_a(line=7)].line == 7
    assert not ds2[test_a(line=4)]
    assert not ds2[test_a(line=8)]
