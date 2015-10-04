# -*- coding: utf-8 -*-

import pytest
from domainics.domobj import dobject, datt, dset

from decimal import Decimal
from datetime import datetime, date

def setup_module(module):
    print()


def test_new_object():

    class A(dobject):
        a = datt(int)
        b = datt(str)
        c = datt(float)
        d = datt(Decimal)
        e = datt(datetime)

        __dobject_key__ = [a, b]

    a1 = A(a=1, b='abc', c=123.4, d='45.67', e='2015-07-29')
    assert a1.e == date(2015, 7, 29)
    assert a1.d == Decimal('45.67')

    a2 = A(dict(a=1, b='abc', c=123.4, d='45.67', e='2015-07-29'), b='xyz')
    assert a1 != a2
    assert a1.a == 1 and a2.a == 1
    assert a1.b == 'abc' and a2.b == 'xyz'

    class obj1():
        a = 10
        b = 'xyz'
    a3 = A(obj1())

    assert a3.a == 10 and a3.b == 'xyz' and a3.c is None
    # print(a3)

    a1 = A()
    a2 = a1._re(a=3)
    assert a1.a is None and a2.a == 3
    with pytest.raises(ValueError) as exc:
        A()._re(123)
