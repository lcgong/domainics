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

        __primary_key__ = [a, b]

    print(A)

    assert len(A._re('a').__value_attrs__) == 1

    with pytest.raises(ValueError) as exc:
        A1 = A._re(_ignore=['a', 'c'], _pkey=['c', 'e']) # c conflict!

    A1 = A._re(_ignore=['a', 'c'], _pkey=['b', 'e']) # c conflict!

    A2 = A._re(c=datt(date, default=date(2015,7,29)))
    print(A.c)
    assert A.c.type == float and A2.c.type == date
    print(A1)
