

# -*- coding: utf-8 -*-

import pytest
from domainics.domobj import dobject, datt, dset

from domainics.domobj.typing import PrimaryKeyTuple, AnyDObject

def setup_module(module):
    print()

class I(dobject):
    s = datt(int)
    n = datt(int)

    x = datt(int)

    __primary_key__ =[s, 'n']


def test_primary_key():
    a = I(s = 1, n=2, x=3)
    b = I(s = 1, n=2, x=5)
    assert a.__primary_key__ == b.__primary_key__
    assert hash(a.__primary_key__) == hash(b.__primary_key__)
    assert isinstance(a.__primary_key__, PrimaryKeyTuple[I])
    assert issubclass(PrimaryKeyTuple[I], PrimaryKeyTuple[AnyDObject])
    # assert isinstance(a.__primary_key__, PrimaryKeyTuple[AnyDObject])
    # some wrong in python 3.5, it's not transitive

    print(b)

def test_equal():
    pass
