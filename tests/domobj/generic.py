

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

from typing import Any, TypeVar

class AA:
    pass
class BB(AA):
    pass

class Obj(DSet[BB]):
    a  = 1


def test_generic_dset():
    # x = Obj()
    # print(isinstance(x, DSet[BB]), isinstance(x, DSet[AA]))
    # a = B(1)
    # print(dir(Obj))
    # print(dir(Obj.__class__))
    # print(dir(dobject.__class__))
    # print(dir(a),'\n\n', dir(B))
    # return
    #
    #

    def f1(d : A):
        print(isinstance(d, DSet))
        print(d)

    def f2(d : DSet[B]):
        print(isinstance(d, DSet))
        print(d)

    ds = DSet(A, [B(1), B(2)])

    f1_arg_d = signature(f1).parameters['d']
    f2_arg_d = signature(f2).parameters['d']

    print(issubclass(f1_arg_d.annotation, DObject))
    print(issubclass(f1_arg_d.annotation, dobject))

    print(f2_arg_d.annotation)
    print(dir(f2_arg_d.annotation))
    print(f2_arg_d.annotation.__origin__, f2_arg_d.annotation.__parameters__)
    print(issubclass(f2_arg_d.annotation, DObject))
    print(issubclass(f2_arg_d.annotation, dobject))
    print(issubclass(f2_arg_d.annotation, dset))

    # hi(1, ds)

    # arg_d = signature(hi).parameters['d']
    # print(arg_d.name, arg_d.annotation, isinstance(arg_d.annotation, DSet))
    # from typing import Any
    #
    # print('s', signature(hi).parameters['s'].annotation)
    #
    # print(dir(arg_d.annotation))
    # print(arg_d.annotation,
    #         arg_d.annotation.__origin__ == DSet[Any].__origin__,
    #         arg_d.annotation.__parameters__)
    #
    # print(DSet[Any], DSet[Any].__origin__)


# t = MySet[Base]
#
# print(t, t())
