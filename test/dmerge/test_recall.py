# -*- coding: utf-8 -*-

import pytest

from datetime import datetime, date
from decimal import Decimal
from domainics.db import dtable
from domainics.domobj import dset, datt

from domainics.db import DBSchema, set_dsn, transaction, dbc, dmerge, drecall

def setup_module(module):
    print()
    set_dsn(sys='postgres', database="ci_test", host='localhost', user='dbo')
    schema = DBSchema()
    schema.add_module(module)
    schema.drop()
    schema.create()


class t_b(dtable):
    a = datt(int)
    b = datt(int)
    c = datt(int)
    d = datt(int)

    __dobject_key__ = [a, b]

@transaction
def test_recall1():

    ASet = dset(t_b, _key=dict(a=datt(int))) # define a key of dset

    ds1 = ASet(a=11)
    ds1 += [t_b(b=12, c=13, d= 14), t_b(b=22, c=23, d=24)]
    dmerge(ds1)

    r1 = drecall(t_b(a=11, b=12))
    assert r1.a == 11 and r1.b == 12 and r1.c == 13
    r1 = drecall(t_b(a=99, b=12)) # no found and return empty dobject
    assert r1 is not None and not bool(r1)


@transaction
def test_recall2():

    ASet = dset(t_b, _key=dict(a=datt(int))) # define a key of dset


    ds1 = ASet(a=1)
    ds1 += [t_b(b=12, c=13, d= 14), t_b(b=22, c=23, d=24)]
    dmerge(ds1)

    ds2 = ASet(a=2)
    ds2 += [t_b(b=32, c=33, d= 34), t_b(b=42, c=43, d=44)]
    dmerge(ds2)

    ds1 = drecall(ASet(a=1))
    ds1 += [t_b(b=12, c=113, d=114), t_b(b=62, c=63, d=64)]
    del ds1[t_b(b=22)]
    with pytest.raises(KeyError):
        del ds1[t_b(b=55)]
    ds0 = drecall(ASet(a=1))
    print("O: ", ds0)
    print("N: ", ds1)
    dmerge(ds1, ds0)
