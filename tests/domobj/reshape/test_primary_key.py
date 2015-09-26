

# -*- coding: utf-8 -*-

import pytest
from domainics.domobj import dobject, datt, dset

from domainics.domobj.typing import PrimaryKeyTuple, AnyDObject

def setup_module(module):
    print()


def test_primary_key():
    class I(dobject):
        s = datt(int)
        n = datt(int)

        x = datt(int)

        __dobject_key__ =[s, 'n']

    a = I(s = 1, n=2, x=3)
    b = I(s = 1, n=2, x=5)
    assert a.__dobject_key__ == b.__dobject_key__
    assert hash(a.__dobject_key__) == hash(b.__dobject_key__)
    assert isinstance(a.__dobject_key__, PrimaryKeyTuple[I])
    assert issubclass(PrimaryKeyTuple[I], PrimaryKeyTuple[AnyDObject])
    # assert isinstance(a.__dobject_key__, PrimaryKeyTuple[AnyDObject])
    # some wrong in python 3.5, it's not transitive

    print(b)

def test_equal():

    class A(dobject):
        a = datt(int)
        b = datt(int)
        c = datt(int)
        d = datt(int)

    assert A(a=1, b=2) == A(a=1, b=2)
    assert A(a=1, b=2, d=3) != A(a=1, b=2, d=4)
    assert A(a=1, b=2, d=3) == A(a=1, b=2, d=3)

    class A(dobject):
        a = datt(int)
        b = datt(int)
        c = datt(int)
        d = datt(int)

        __dobject_key__ = [a, b]

    assert A(a=1, b=2) == A(a=1, b=2)
    assert A(a=1, b=2, d=3) == A(a=1, b=2, d=4) # d irrevalent!
    assert A(a=1, b=2, d=3) == A(a=1, b=2, d=3)

def test_bool():

    assert not dobject()

    class A(dobject):
        pass

    assert not A()

    class A(dobject):
        a = datt(int)
        b = datt(int)
        c = datt(int, default=100)
        d = datt(int)

    assert not A()
    assert not A(a=None)
    assert A().c == 100
    assert not A(c=100)
    assert A(a=1)

    with pytest.raises(ValueError) as exc:
        assert A(b=None) == False
    print(exc)
