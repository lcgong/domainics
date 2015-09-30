# -*- coding: utf-8 -*-

import pytest

from datetime import datetime, date
from decimal import Decimal
from domainics.db import dtable, tcol
from domainics.domobj import dset, reshape



def setup_module(module):
    print()

class t_a(dtable):
    a = tcol(int)
    b = tcol(int)
    c = tcol(int, doc="should be constrained")
    d = tcol(int)
    e = tcol(int, doc="should be constrained")
    f = tcol(int)

    __dobject_key__ = [a, b]

def test_diff():

    from domainics.db.dmerge import _dtable_diff as diff

    # orginal data
    ds = dset(t_a, [
            t_a(10010, 10011, 10012, 10013, 10014),
            t_a(10010, 10021, 10022, 10023, 10024),
            ])

    # constrained information in this domain,
    # reshape t_a to t_a1
    t_a1 = reshape(t_a, _ignore=('c', 'e'), _name='t_a1')
    ds1 = dset(t_a1, ds)

    inslst, updlst, dellst  = diff(ds1)

    assert len(inslst.values) == 2 and len(inslst.pkey_values) == 2
    assert len(updlst.values) == 0 and len(updlst.pkey_values) == 0
    assert len(dellst.pkey_values) == 0

    # print('INS:\n', inslst, '\nUPD:\n', updlst, '\nDEL:\n', dellst)

    ds1_1 = ds1.copy()

    ds1_1[0].f = 100
    del ds1_1[1]

    inslst, updlst, dellst  = diff(ds1_1, ds1)
    print('INS:\n', inslst, '\nUPD:\n', updlst, '\nDEL:\n', dellst)
