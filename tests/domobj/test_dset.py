# -*- coding: utf-8 -*-


import decimal
import datetime as dt

from domainics.domobj import dobject, datt, dset


import pytest


def setup_module(module):
    print()


def test_dset_primary_key():
    class A(dobject):
        a = datt(int)
        b = datt(int)

    # infomation of primary_key in dset is requried
    with pytest.raises(ValueError) as ex:
        dset(A, [A(1,2)])


    class A(dobject):
        a = datt(int)
        b = datt(int)
        c = datt(int)

        __dobject_key__ = [a]


    a_list = [A(a=1, b=1, c=2), A(a=1, b=2, c=3)]
    s = dset(A)
    # Because the primary_key is 'a' attribute, the two above instances
    # have the same value in this dset, their are regarded to be the one.
    # Thus the later instance will overwrite the previous

    # s += a_list

    assert len(s.__item_key__) == 1

    s.add(a_list[0])


    assert len(s) == 1
    assert s[0].c == 2
    assert len(s[0].__dobject_key__) == 1
    assert s[0].__dobject_key__.a == 1

    s.add(a_list[1])
    assert len(s) == 1
    assert s[0].c == 3


    s = dset(A, item_key=[A.a, A.b])
    s += a_list
    assert len(s) == 2
    assert s[0].c == 2 and s[1].c == 3
