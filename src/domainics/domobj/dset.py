# -*- coding: utf-8 -*-

from collections import OrderedDict
from collections import namedtuple
from collections.abc import Iterable, Mapping

import sys

# from .metaclass import datt
# from ._reshape import reshape

from .dobject import dobject
from .typing import DObject, DSet, DAttribute, AnyDObject
from .typing import parse_attr_value_many
from .metaclass import _make_pkey_class, DObjectMetaClass


_dset_class_tmpl = """\
class {typename}(DSetBase):
    pass
"""
def dset(*args, **kwargs):

    if len(args) != 1:
        errmsg = "dset(item_type, ...), item_type is required"
        raise ValueError(errmsg)

    item_type = args[0]

    dset_key = OrderedDict()
    dominion_class = None

    arg_name = '_key'
    if arg_name in kwargs:
        arg_value = parse_attr_value_many(kwargs.pop(arg_name), arg_name)
        dset_key.update(arg_value)


    arg_name = '_dominion'
    if arg_name in kwargs:
        arg_value = kwargs.pop(arg_name)
        if not isinstance(arg_value, type):
            raise  ValueError()
        dominion_class = arg_value


    links = OrderedDict()
    for attr_name, attr in kwargs.items():
        if not isinstance(attr, DAttribute):
            errmsg = "The value of link '%s' must be an attribue object"
            errmsg %= attr_name
            raise ValueError(errmsg)

    if not dset_key and dominion_class:
        dset_key = dominion_class.__dobject_key__

    # ----------------------------------------------------------------------

    typename = item_type.__name__ + '_dset'
    class_code = _dset_class_tmpl.format(typename = typename)

    namespace = dict(DSet = DSet, DSetBase=DSetBase, T = DObject)
    exec(class_code, namespace)
    dset_cls = namespace[typename]
    # pkey_cls.__module__ = dobj_cls.__module__

    for attr_name, attr in dset_key.items():
        setattr(dset_cls, attr_name, attr)

    dset_cls.__dset_item_class__ = item_type
    dset_cls.__dominion_class__ = dominion_class
    dset_cls.__dobject_key__ = dset_key
    dset_cls.__dobject_key_class__ = _make_pkey_class(dset_cls)
    dset_cls.__dobject_att__ = OrderedDict()

    print(dset_key, dominion_class)

    try:
        frame = sys._getframe(1)
        dset_cls.__module__ = frame.f_globals.get('__name__', '__main__')
    except (AttributeError, ValueError):
        pass


    return dset_cls


class DSetBase(dobject):
    """The set of dobjects.
    """
    # __dset_item_class__ = None
    # __dominion_class__ = None
    # __dobject_key__ = None

    def __new__(cls, *args, **kwargs):

        instance = super(DSetBase, cls).__new__(cls, **kwargs)

        instance_setter = super(dobject, instance).__setattr__
        instance_setter('__dset_item_dict__',  OrderedDict())
        instance_setter('__dominion_object__',  None)

        if args:
            instance += args[0]

        print('333', instance.a, instance.__dobject_key__)

        print(444, instance.__dobject_key_class__,
        instance.__dobject_key_class__(instance))

        return instance

    def _add(self, obj):
        """
        If the identity of obj has been added, replace the old one with it.
        """
        # print(obj)
        obj = self.__dset_item_class__(obj)
        key = obj.__dobject_key__
        self.__dset_item_dict__[key] = obj

        return self

    def _clear(self):
        """clear all objects in aggregate"""

        self.__dset_item_dict__.clear()

        return self


    def __json_object__(self):
        """export dset object in list"""

        return [item.__json_object__() for item in self.__dset_item_dict__.values()]

    def __bool__(self):
        return bool(self.__dset_item_dict__)

    def __len__(self):
        return len(self.__dset_item_dict__)

    def __iter__(self):
        for item in self.__dset_item_dict__.values():
            yield item

    def __repr__(self):

        opts = []
        print(333, len(self.__class__.__dobject_key__), len(self.__dobject_key__))

        if self.__class__.__dobject_key__:
            opts.append(repr(self.__dobject_key__))

        opts.append(repr([item for item in self.__dset_item_dict__.values()]))

        opts.append("_item_type={0!s}".format(
                            self.__dset_item_class__.__name__))

        if self.__dominion_class__:
            opts.append("_dominion={0!s}".format(
                            self.__dominion_class__.__name__))

        if self.__dobject_key__:
            opts.append("_key={0!r}".format(self.__dobject_key__))

        opts = ', '.join(opts)

        s = ("{typename}({opts})").format(
                        typename = self.__class__.__name__, opts = opts)

        return s

    def __getitem__(self, index):

        if isinstance(index, DObject):
            obj = self.__dset_item_dict__.get(index.__dobject_key__, None)
            if obj is not None:
                return obj

        elif isinstance(index, self.__dset_item_class__.__dobject_key_class__):
            obj = self.__dset_item_dict__.get(index, None)
            if obj is not None:
                return obj

        else:
            index = self.__dset_item_class__(index)
            obj = self.__dset_item_dict__.get(index.__dobject_key__, None)
            if obj is not None:
                return obj

        return self.__dset_item_class__()

    def __delitem__(self, index):

        if isinstance(index, DObject):
            del self.__dset_item_dict__[index.__dobject_key__]

        elif isinstance(index, self.__dset_item_class__.__dobject_key_class__):
            del self.__dset_item_dict__[index]

        else:
            index = self.__dset_item_class__(index)
            del self.__dset_item_dict__[index.__dobject_key__]

    def __setitem__(self, index, value):

        item_type = self.__dset_item_class__

        if isinstance(index, DObject):
            self.__dset_item_dict__[index.__dobject_key__] = item_type(value)

        elif isinstance(index, item_type.__dobject_key_class__):
            self.__dset_item_dict__[index] = item_type(value)

        else:
            index = item_type(index)
            self.__dset_item_dict__[index.__dobject_key__] = item_type(value)


    def __hash__(self):
        return hash(self)

    def __eq__(self, other):
        if isinstance(other, dset):
            other_iter = other.__list

        elif isinstance(other, list) or isinstance(other, tuple):
            other_iter = other

        else:
            return False

        if len(self.__list) != len(other_iter):
            return False

        return all(a == b for a, b in zip(self.__list, other_iter))


    def __iadd__(self, value) :

        if isinstance(value, (DSet[DObject], Iterable)):
            for obj in value:
                self._add(obj)
        elif isinstance(value, DObject):
            self._add(value)

        return self  # operator 'o.x += a', o.x = o.x.__iadd__(a)
