# -*- coding: utf-8 -*-

import pytest
from domainics.domobj import dobject, datt, dset

def setup_module(module):
    print()

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

def test_reshape_dobject_class():
    a = I(s = 1, n=2)
    print(a)
    I._re('a')

    I()._re(a=1)
