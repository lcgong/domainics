# -*- coding: utf-8 -*-

import pytest

from datetime import datetime, date
from decimal import Decimal
from domainics.db import dtable
from domainics.domobj import dset, datt



def setup_module(module):
    print()

class t_a(dtable):
    a = datt(int)
    b = datt(int)
    c = datt(int, doc="should be constrained")
    d = datt(int)
    e = datt(int, doc="should be constrained")
    f = datt(int)

    __dobject_key__ = [a, b]

def test_diff():

    from domainics.db.dmerge import _dtable_diff

    # orginal data
    ASet = dset(t_a)
    ds = ASet([
            t_a(a=10010, b=10011, c=10012, d=10013, e=10014),
            t_a(a=10010, b=10021, c=10022, d=10023, e=10024),
            ])

    # constrained information in this domain,
    # reshape t_a to t_a1

    t_a1 = t_a._re(_ignore=('c', 'e'), _name='t_a1')

    A1Set = dset(t_a1)

    ds1 = A1Set(ds)
    ds0 = A1Set()
    inslst, updlst, dellst  = _dtable_diff(ds1, ds0)

    assert len(inslst.values) == 2 and len(inslst.pkey_values) == 2
    assert len(updlst.values) == 0 and len(updlst.pkey_values) == 0
    assert len(dellst.pkey_values) == 0

    # print('INS:\n', inslst, '\nUPD:\n', updlst, '\nDEL:\n', dellst)

    ds2 = A1Set(ds1) # clone it

    ds2[t_a(a=10010, b=10011)].f = 100
    del ds2[t_a(a=10010, b=10021)]

    inslst, updlst, dellst  = _dtable_diff(ds2, ds1)
    print('INS:\n', inslst, '\nUPD:\n', updlst, '\nDEL:\n', dellst)

    assert len(inslst.values) == 0 and len(inslst.pkey_values) == 0
    assert len(updlst.values) == 1 and len(updlst.pkey_values) == 1
    assert len(dellst.pkey_values) == 1
