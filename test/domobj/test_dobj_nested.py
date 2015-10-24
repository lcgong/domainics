# -*- coding: utf-8 -*-

import pytest
from domainics.domobj import dobject, datt, dset

from decimal import Decimal
from datetime import datetime, date

def setup_module(module):
    print()


def test_new_object():

    class B(dobject):
        a = datt(int)
        b = datt(int)
        c = datt(int)

        __dobject_key__ = [a, b]

    class A(dobject):
        a = datt(int)

        x = datt(dset(B))

        __dobject_key__ = [a]
    a1 = A(a=1)
    a1.x = [B(a=11, b=21, c=31), B(a=12, b=22, c=32), B(a=13, b=23, c=33)]
    a2 = A(a1)

    print(a1)
    print(a2)

    assert len(a1.x) == 3
    assert len(a2.x) == 3
