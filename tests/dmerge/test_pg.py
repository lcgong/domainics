# -*- coding: utf-8 -*-

import pytest

from datetime import datetime, date
from decimal import Decimal
from domainics.db import dtable, tcol
from domainics.domobj import dset, reshape

from domainics.db import DBSchema, set_dsn, transaction, dbc, dmerge

def setup_module(module):
    print()
    set_dsn(sys='postgres', database="demo", host='localhost', user='postgres')
    schema = DBSchema()
    schema.add_module(module)
    schema.drop()
    schema.create()

class t_a(dtable):
    a = tcol(int)
    b = tcol(int)
    c = tcol(int, doc="should be constrained")
    d = tcol(int)
    e = tcol(int, doc="should be constrained")
    f = tcol(int)

    __primary_key__ = [a, b]

@transaction
def test_diff():

    # orginal data
    ds = dset(t_a, [
            t_a(10010, 10011, 10012, 10013, 10014),
            t_a(10010, 10021, 10022, 10023, 10024),
            ])

    # constrained information in this domain,
    # reshape t_a to t_a1
    t_a1 = reshape(t_a, _ignore=('c', 'e'))
    ds1 = dset(t_a1, ds)

    dmerge(ds1)

    dbc << "SELECT * FROM t_a"
    ds0 = dset(t_a1, dbc)
    ds1 = ds0.copy()
    ds1.add(t_a1(a=20010, b=10011, d=20013, f=3343))
    ds1[0].f = 5555
    print(333, ds1[1])
    print(ds1)
    del ds1[1]
    print(ds1)

    dmerge(ds1, ds0)
