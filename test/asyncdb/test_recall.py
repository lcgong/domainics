# -*- coding: utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


import pytest
import inspect
from datetime import datetime, date
from decimal import Decimal

from domainics.domobj import dobject, datt, dset
from domainics.sqltext import SQL
from domainics.asyncdb.sqlblock import set_dsn, transaction
from domainics.domobj import dobject, datt, dset
from domainics.asyncdb.dtable import dtable, dsequence
from domainics.asyncdb.schema import DBSchema


def setup_module(module):
    set_dsn(dsn='testdb', url="postgresql://postgres@localhost/test")

def setup_function(function):
    set_dsn(dsn='testdb', url="postgresql://postgres@localhost/test")
    module = inspect.getmodule(function)

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

@transaction.db(dsn='testdb')
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

@transaction
def test_recall_without_dset_key():
    ASet = dset(t_b) # define a key of dset

    ds1 = ASet()
    ds1 += [t_b(a=1, b=12, c=13, d= 14), t_b(a=1, b=22, c=23, d=24)]
    ds1 += [t_b(a=1, b=32, c=33, d= 34), t_b(a=1, b=42, c=43, d=44)]
    dmerge(ds1)

    ds2 = drecall(ASet())
    assert len(ds2) == 4
    print(ds2)
