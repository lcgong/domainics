# -*- coding: utf-8 -*-

import pytest

from datetime import datetime, date
from decimal import Decimal
from domainics.db import dtable, dsequence
from domainics.domobj import dset, datt

from domainics.db import DBSchema, set_dsn, transaction, dbc, dmerge, drecall

def setup_module(module):
    print()
    set_dsn(sys='postgres', database="ci_test", host='localhost', user='dbo')
    schema = DBSchema()
    schema.add_module(module)
    schema.drop()
    schema.create()

class seq_test(dsequence):
    start = 10000
    step  = 1

class t_a(dtable):
    a = datt(seq_test)
    b = datt(int)

    __dobject_key__ = [a]

@transaction
def test_sequence():

    r1 = t_a(b=101)
    r2 = t_a(b=102)
    print('R1: ', r1, '\nR2: ', r2, sep='')
    assert isinstance(r1.a, seq_test) and  isinstance(r2.a, seq_test)
    assert r1.a is not r2.a
    assert r1.a != r2.a

    ASet = dset(t_a)
    s1 = ASet()
    s1 += [r1, r2]
    dmerge(s1)

    print('R1: ', r1, '\nR2: ', r2, sep='')
    assert isinstance(r1.a, seq_test) and  isinstance(r2.a, seq_test)
    assert r1.a is not r2.a
    assert r1.a != r2.a
    assert r1.a.value == 10000 and int(r1.a) == 10000
    assert r2.a.value == 10001 and int(r2.a) == 10001


    dbc << "SELECT * FROM t_a ORDER BY a"
    s1 = ASet(dbc)
    sl = list(s1)
    print(s1)
    assert isinstance(sl[0].a, seq_test)
    assert sl[0].a.value == 10000
    assert sl[0].a == r1.a
