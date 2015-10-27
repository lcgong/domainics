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
    class B(dobject):
        x = datt(int)
        y = datt(int)
        __dobject_key__ = [x]

    class A(dobject):
        a = datt(int)
        b = datt(dset(B))
        __dobject_key__ = [a]

    a = A(a=1)

    a.b._add(B(x=1, y=11))
    a.b._add(B(x=2, y=21))
    a.b._add(B(x=3, y=31))

    print(a)
    assert len(a.b) == 3
