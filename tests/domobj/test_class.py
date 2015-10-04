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

    A1 = A._re('a')
    assert len(A1.__dobject_key__) == 1 and len(A1.__dobject_att__) == 0

    with pytest.raises(ValueError) as exc:
        A1 = A._re(_ignore=['a', 'c'], _key=['c', 'e']) # c conflict!

    A1 = A._re(_ignore=['a', 'c'], _key=['b', 'e'])
    A2 = A._re(c=datt(date, default=date(2015,7,29)))
    assert A.c.type == float and A2.c.type == date

    A3 = A._re('a', 'b', 'c', _subst=dict(a='s'))
    assert tuple(A3.__dobject_key__) == ('s', 'b')
    assert tuple(A3.__dobject_att__) == ('c',)
    print(A3(s=1, b=2, c=3))


def test_subst_reshape():

    class A(dobject):
        a = datt(int)
        b = datt(int)
        c = datt(int)

        __dobject_key__ = [a, b]

    A1 = A._re(_subst = dict( b = 's'), _name='A1')
    with pytest.raises(ValueError):
        a1 = A1(a=1, b=2, c=3)

    a1 = A1(a=1, s=2, c=3)
    assert a1.a == 1 and a1.s == 2 and a1.c == 3

    A1 = A._re(_subst = dict(b = 's'), b = datt(float), _name = 'A1')

    a1 = A1(A(a=1, b=2, c=3))
    print(a1)
    assert a1.a == 1 and a1.s == 2 and a1.b is None and a1.c == 3

    # switch
    a = A(a=1, b=2, c=3)
    A2 = A._re(_subst = dict(b = 'c', c='b'), b = datt(float), _name = 'A2')
    a = A(a=4, b=5, c=6)
    a2 = A2(a)
    assert a.b == 5 and a.c == 6
    assert a2.b == 6 and a2.c == 5
