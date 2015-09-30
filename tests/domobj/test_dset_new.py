# -*- coding: utf-8 -*-


import decimal
import datetime as dt

from domainics.domobj import dobject, datt, dset
from domainics.domobj.dset import dset

from domainics.domobj.typing import DSet, DObject, AnyDObject

import pytest


def setup_module(module):
    print()

@pytest.mark.skipif
def test_dset_declaration1():
    class A(dobject):
        a = datt(int)
        b = datt(int)
        __dobject_key__ = [a]

    class B(dobject):
        x = datt(int)
        y = datt(int)
        __dobject_key__ = [x]


    # A <>---> B
    BSet = dset(B, _key=A.a)
    assert tuple(BSet.__dobject_key__) == ('a',)
    assert BSet.__dominion_class__ == A
    assert BSet.a != A.a
    assert BSet.a.owner_class == BSet

    BSet = dset(B, _dominion=A, _key=['a'])
    assert tuple(BSet.__dobject_key__) == ('a',)
    assert BSet.__dominion_class__ == A
    assert BSet.a != A.a
    assert BSet.a.owner_class == BSet

    with pytest.raises(ValueError):
        BSet = dset(B, _key=['a']) # the attribute a is undefined

    BSet = dset(B, _key=dict(a=datt(int)))
    assert tuple(BSet.__dobject_key__) == ('a',)
    assert BSet.__dominion_class__ is None
    assert BSet.a != A.a
    assert BSet.a.owner_class == BSet

@pytest.mark.skipif
def test_dset_declaration2():

    class B(dobject):
        x = datt(int)
        y = datt(int)

    with pytest.raises(TypeError):
        BSet = dset(B)

    class B(dobject):
        x = datt(int)
        y = datt(int)
        __dobject_key__ = x

    BSet = dset(B)

    s1 = BSet([B(x=1, y=11), B(x=2, y=22)])
    s1 += [B(x=2, y=22)]
    s1 += [B(x=3, y=23)]

    with pytest.raises(ValueError):
        s1 = BSet(s1, e=1)


# @pytest.mark.skipif
def test_dset_declaration3():

    class A(dobject):
        a = datt(int)
        b = datt(int)
        __dobject_key__ = [a]

    class B(dobject):
        x = datt(int)
        y = datt(int)
        __dobject_key__ = [x]

    BSet = dset(B, _key=A.a)
    s1 = BSet([B(x=1, y=11), B(x=2, y=22)])
    assert s1.a is None

    s1 = BSet(s1, a = 101)
    assert s1.a == 101

    #-------------------------------------------------------------------
    class B(dobject):
        a = datt(int)
        x = datt(int)
        y = datt(int)
        __dobject_key__ = [x]

    BSet = dset(B, _key=A.a)
    s1 = BSet([B(x=1, y=11), B(x=2, y=22)], a=101)
    sl = list(s1)
    assert s1.a == 101 and sl[0].a == 101 and sl[1].a == 101

    class B(dobject):
        x = datt(int)
        y = datt(int)
        __dobject_key__ = [x]

def test_dset_links():
    
    class A(dobject):
        a = datt(int)
        b = datt(int)
        __dobject_key__ = [a]

    class B(dobject):
        x = datt(int)
        y = datt(int)
        __dobject_key__ = [x]

    BSet = dset(B, _key=A.a, y=A.a)


#
# def test_dset_3():
#     class Item(dobject):
#         line_no = datt(int)
#         item_sn = datt(int)
#         __dobject_key__ = [line_no]
#
#     class Bill(dobject):
#         sn = datt(int)
#         items = datt(dset(Item))
#         __dobject_key__ = [sn]
#
#
#     ASet = dset(Item)._re(_key=1)
#     print(ASet)
