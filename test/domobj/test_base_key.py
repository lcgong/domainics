# -*- coding: utf-8 -*-


import decimal
import datetime as dt

from domainics.domobj import dobject, datt, dset
from domainics.domobj.dset import dset

from domainics.domobj.typing import DSet, DObject, AnyDObject

import pytest


def setup_module(module):
    print()

# @pytest.mark.skipif
def test_dset_declaration1():
    class A(dobject):
        a = datt(int)
        b = datt(int)
        __dobject_key__ = [a]

    class B(dobject):
        a = datt(int)
        b = datt(int)
        __dobject_key__ = [b]

    class C(A,B):
        c = datt(int)
        __dobject_key__ = [c]

    c = C(a=1, b=2, c=3)

    assert list(C.__dobject_key__.values()) == [C.c]
    c.a = 11 # it is not pk anymore
    assert c.a == 11
