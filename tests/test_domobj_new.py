

import decimal
import datetime as dt

from domainics.domobj import dobject, datt, dset


import pytest


def setup_module(module):
    print()


def test_reform():

    class I(dobject):
        s = datt(int)
        z = datt(int)

        __primary_key__ = s

    class A(dobject):
        a = datt(int)
        b = datt(int)
        c = datt(int)
        d = datt(int)

        l = dset(item_type=I)

        x = datt(int)

        __primary_key__ = [a, b]

    class B(dobject):
        a = datt(int)
        b = datt(int)
        c = datt(int)
        d = datt(int)

        y = datt(int)

        l = dset(item_type=I)

        __primary_key__ = [a, b]


    a = A(a=1, b=2, c=3, d=4, x=8)
    a.l = dset(I, [I(1, 101), I(2, 102)])


    b = A(a=13, c=13)
    print(a)
    print(b)

    b.reform(a)
    print(b)



@pytest.mark.skipif(True)
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
