# -*- coding: utf-8 -*-



import decimal
import datetime as dt
from domainics.domobj import dobject, datt, dset, transform


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
    b1 = B(transform(a1, ignore=A.x), b=2, c=3)
    assert a1.x == 13 and b1.x is None
    assert a1.b == 12 and b1.b == 2
    assert a1.a == b1.a
    # assert a1.l == b1.l

    print(a1)
    print(b1)
