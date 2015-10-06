# -*- coding: utf-8 -*-

import pytest
from domainics.domobj import dobject, datt, dset

from decimal import Decimal
from datetime import datetime, date

def setup_module(module):
    print()

def test_json():

    class B(dobject):
        a_sn = datt(int)
        line = datt(int)
        name = datt(str)
        __dobject_key__ = [a_sn, line]

    class A(dobject):
        a_sn = datt(int)
        b = datt(dset(B))
        __dobject_key__ = [a_sn]


    a = A(a_sn = 101)
    a.b += [B(line=1, name='n1'), B(line=2, name='n2')]

    json1 = a.__json_object__()

    assert json1['a_sn'] == 101
    assert json1['b'][0]['a_sn'] == 101
    assert json1['b'][0]['line'] == 1
    assert json1['b'][1]['a_sn'] == 101
    assert json1['b'][1]['line'] == 2
