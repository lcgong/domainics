

import decimal
import datetime as dt

from domainics.domobj import dobject, datt, dset


import pytest


from typing import TypeVar, Iterable, Generic

# class Base:
#     pass
#
# T = TypeVar('T', Base)
#
# class MySet(Generic[T]):
#     pass


def setup_module(module):
    print()

class A(dobject):
    s = datt(int)

    __primary_key__ = s

class B(A):
    pass

from domainics.domobj.metaclass import DObject, DSet
from inspect import signature

def test_generic_dset():

    def hi(s, d : DSet[B]):
        print(isinstance(d, DSet))
        print(d)

    ds = DSet(A, [B(1), B(2)])

    hi(1, ds)

    arg_d = signature(hi).parameters['d']
    print(arg_d.name, arg_d.annotation, isinstance(arg_d.annotation, DSet))
    from typing import Any

    print('s', signature(hi).parameters['s'].annotation)

    print(dir(arg_d.annotation))
    print(arg_d.annotation,
            arg_d.annotation.__origin__ == DSet[Any].__origin__,
            arg_d.annotation.__parameters__)

    print(DSet[Any], DSet[Any].__origin__)


# t = MySet[Base]
#
# print(t, t())
