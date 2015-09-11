
import unittest

import decimal
import datetime as dt

from domainics.domobj import dobject, datt, dset


import pytest


def setup_module(module):
    print()

def test_new():

    class A(dobject):
        s = datt(int)
        a = datt(int, expr="100")

        __primary_key__ = [s]


    class B(A):
        b = datt(int, expr='201')
        m = datt(int, expr='202')


    class C(B):
        c = datt(int, expr='300')

    # print('C', C.__primary_key__, 'value = ',C.__value_attrs__)

    c = C(c=310, b=210, a=150)
    print(c)



    # assert c.c == 310
    # assert c.b == 210
    # assert c.m == 202
    # assert c.a == 150

    # with pytest.raises(ValueError) as exc:
    #     c = C(310, x=150) # the attribute x is not defined

    # print(exc)
