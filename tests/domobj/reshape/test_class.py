# -*- coding: utf-8 -*-

import pytest
from domainics.domobj import dobject, datt, dset

from decimal import Decimal
from datetime import datetime, date

def setup_module(module):
    print()


def test_new_object():

    class A(dobject, keys=['a']):
        a = datt(int)
        b = datt(str)
        c = datt(float)
        d = datt(Decimal)
        e = datt(datetime)

        __primary_key__ = [a, b]
        # __dobject_key__ = [a, b]
        # __dobject_att__

    A1 = A._re('a')
    assert len(A1.__primary_key__) == 1 and len(A1.__value_attrs__) == 0

    with pytest.raises(ValueError) as exc:
        A1 = A._re(_ignore=['a', 'c'], _pkey=['c', 'e']) # c conflict!

    A1 = A._re(_ignore=['a', 'c'], _pkey=['b', 'e'])
    A2 = A._re(c=datt(date, default=date(2015,7,29)))
    assert A.c.type == float and A2.c.type == date

    A3 = A._re('a', 'b', 'c', _subst=dict(a='s'))
    assert tuple(A3.__primary_key__) == ('s', 'b')
    assert tuple(A3.__value_attrs__) == ('c',)
    print(A3(s=1, b=2, c=3))
    
