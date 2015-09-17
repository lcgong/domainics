# -*- coding: utf-8 -*-


import decimal
import datetime as dt
from domainics.domobj import dobject, datt, dset, reshape


import pytest

def setup_module(module):
    print()

def test_transformation():
    class I(dobject):
        s = datt(int)

    class A(dobject):
        a = datt(int)
        b = datt(int)
        x = datt(int)
        l = dset(item_type=I, primary_key=I.s)

        __primary_key__ = [a]

    class B(A):
        c = datt(int)
        d = datt(int)

        __primary_key__ = [A.a, c]


    a1 = A(a=11, b=12, x=13, l=[I(100), I(101)])
    b1 = B(reshape(a1, ignore=A.x), b=2, c=3)
    assert a1.x == 13 and b1.x is None
    assert a1.b == 12 and b1.b == 2
    assert a1.a == b1.a
    assert a1.l == b1.l

    b2 = B(reshape(dict(a=1, b=2, xx=123, l=[dict(s=201), dict(s=202)])))
    assert b2.a == 1 and b2.b == 2
    assert len(b2.l) == 2
    assert b2.l[I(202)].s == 202

    print(b2)

    # print(a1)
    # print(b1)
