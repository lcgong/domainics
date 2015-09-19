# -*- coding: utf-8 -*-

import pytest
from domainics.domobj import dobject, datt, dset, reshape

def setup_module(module):
    print()

def test_reshape_dobject_class():
    class I(dobject):
        s = datt(int)
        n = datt(int)

    class A(dobject):
        a = datt(int)
        b = datt(int)
        c = datt(int)
        d = datt(int)
        e = datt(int)
        l = dset(item_type=I, primary_key=I.s)

        __primary_key__ = [a, b]

    A1 = reshape(A, _ignore=(A.e, A.d), _primary_key=A.a)

    assert 'e' not in A1.__value_attrs__
    assert 'd' not in A1.__value_attrs__
    assert tuple(A1.__primary_key__.keys()) == ('a',)
