# -*- coding: utf-8 -*-


import decimal
import datetime as dt

from domainics.domobj import dobject, datt, dset


import pytest


def setup_module(module):
    print()


def test_dset():

    class A(dobject):
        a = datt(int)
        b = datt(int)
        c = datt(int)
        
        __primary_key__ = [a]


    a = A(a=1, b=1, c=2)
    b = A(a=1, b=2, c=3)

    s = dset(A)

    s.append(a)
    s.append(b)
    print(s)

    s = dset(A, primary_key=[A.a, A.b])
    s.append(a)
    s.append(b)
    print(s)
    



