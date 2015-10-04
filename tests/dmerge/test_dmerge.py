# -*- coding: utf-8 -*-

import pytest

from datetime import datetime, date
from decimal import Decimal
from domainics.db import dtable
from domainics.domobj import dset, datt

from domainics.db import DBSchema, set_dsn, transaction, dbc, dmerge

def setup_module(module):
    print()
    set_dsn(sys='postgres', database="ci_test", host='localhost', user='dbo')
    schema = DBSchema()
    schema.add_module(module)
    schema.drop()
    schema.create()

class t_a(dtable):
    a = datt(int)
    b = datt(int)
    c = datt(int, doc="should be constrained")
    d = datt(int)
    e = datt(int, doc="should be constrained")
    f = datt(int)

    __dobject_key__ = [a, b]

@transaction
def test_dobject_dmerge():

    # orginal data
    ASet = dset(t_a)

    ds = ASet([t_a(a=101, b=102, c=103, d=104, e=105),
               t_a(a=201, b=202, c=203, d=204, e=205)])

    t_a1 = t_a._re(_ignore=['c', 'e'])
    A1Set = dset(t_a1)

    ds1 = A1Set(ds)

    dmerge(ds1)

    dbc << "SELECT * FROM t_a"
    ds0 = A1Set(dbc) # original
    ds1 = A1Set(ds0) # to be modified

    ds1 += [t_a1(a=301, b=302, d=304, f=306)] # insert a new item
    ds1[t_a(a=101, b=102)].f = 5555 # update the first item
    del ds1[t_a(a=201, b=202)] # delete the 2nd item

    print('ORIGINAL:', ds0)
    print('MODIFIED:', ds1)

    dmerge(ds1, ds0)
