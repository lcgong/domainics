# -*- coding: utf-8 -*-

import pytest

from datetime import datetime, date
from decimal import Decimal
from domainics.db import dtable, array
from domainics.domobj import dset, datt

from domainics.db import DBSchema, set_dsn, transaction, dbc, dmerge, drecall

def setup_module(module):
    print()
    set_dsn(sys='postgres', database="ci_test", host='localhost', user='dbo')
    schema = DBSchema()
    schema.add_module(module)
    schema.drop()
    schema.create()


class t_a(dtable):
    a = datt(int)
    b = datt(array(str, dimensions=1))

    __dobject_key__ = [a]

@transaction
def test_array():

    ASet = dset(t_a)
    s1 = ASet()

    r1 = t_a(a=1)
    s1 += [r1]
    s1[0].b = ['a', 'b']
    assert s1[0].b == ['a', 'b']

    dmerge(s1)

    dbc << "SELECT * FROM t_a ORDER BY a"
    s2 = ASet(dbc)
    assert s2[0].b == ['a', 'b']

    print(s2)
