# -*- coding: utf-8 -*-


import decimal
import datetime as dt

from domainics.domobj import dobject, datt, dset
from domainics.domobj.dset import dset

from domainics.domobj.typing import DSet, DObject, AnyDObject

import pytest


def setup_module(module):
    print()


def test_dset_1():
    class A(dobject):
        a = datt(int)
        b = datt(int)
        __dobject_key__ = [a]

    class B(dobject):
        x = datt(int)
        y = datt(int)
        __dobject_key__ = [x]


    # A <>---> B
    BSet = dset(B, _dominion=A, _key=A.a)

    a = A(a=101, b =102)

    from domainics.domobj import DObject

    assert isinstance(a, DObject)

    s1 = BSet(_dominion=a)
    s1 += [B(x=1, y=11), B(x=2, y=22), B(x=2, y=22)]


    print(s1)

    #
