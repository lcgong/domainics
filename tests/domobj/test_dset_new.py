# -*- coding: utf-8 -*-


import decimal
import datetime as dt

from domainics.domobj import dobject, datt, dset
from domainics.domobj.dset import dset

from domainics.domobj.typing import DSet, DObject, AnyDObject

import pytest


def setup_module(module):
    print()

# @pytest.mark.skipif
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

# @pytest.mark.skipif
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

# @pytest.mark.skipif
def test_dset_clone():
    class B(dobject):
        x = datt(int)
        y = datt(int)
        __dobject_key__ = [x]

    BSet = dset(B)
    ds1 = BSet([B(x=11, y=12), B(x=21,y=22)])
    ds2 = BSet(ds1)

    ls1 = list(ds1)
    ls2 = list(ds2)

    ls2[0].y = 102

    assert ls1[0].y == 12 and ls2[0].y == 102


# @pytest.mark.skipif
def test_dset_links_1():

    class A(dobject):
        a = datt(int)
        b = datt(int)
        __dobject_key__ = [a]

    class B(dobject):
        x = datt(int)
        y = datt(int)
        __dobject_key__ = [x]

    BSet = dset(B, _key=A.a, a='y') # link
    s1 = BSet([B(x=1, y=11), B(x=2, y=22)], a=101)
    sl = list(s1)
    assert s1.a == 101 and sl[0].y == 101 and sl[1].y == 101

    print(s1)
    #-------------------------------------------------------------------
    class B(dobject):
        a = datt(int) # the same name with dset key
        x = datt(int)
        y = datt(int)
        __dobject_key__ = [x]

    BSet = dset(B, _key=A.a, a='y') # link
    s1 = BSet([B(x=1, y=11), B(x=2, y=22)], a=101)
    sl = list(s1)
    assert s1.a == 101 and sl[0].y == 101 and sl[1].y == 101
    assert sl[0].a is None and sl[1].a is None

    print(s1)

def test_dset_datt():
    class Item(dobject):
        bill_sn = datt(int)
        line_no = datt(int)
        item_sn = datt(int)

        __dobject_key__ = [line_no]

    class Bill(dobject):
        sn = datt(int)
        __dobject_key__ = [sn]
        items = datt(dset(Item, sn='bill_sn'), doc="")

    bill = Bill(sn=101)
    assert tuple(bill.items.__dobject_key__) == (101,)
    assert bill.items.sn == 101
    bill.items._add(Item(line_no=1, item_sn=301))
    sl = list(bill.items)
    assert bill.sn == 101 and sl[0].bill_sn == 101
    bill.items += [Item(line_no=1, item_sn=301), Item(line_no=2, item_sn=302)]
