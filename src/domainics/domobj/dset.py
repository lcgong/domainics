# -*- coding: utf-8 -*-

from collections import OrderedDict
from collections import namedtuple
from collections.abc import Iterable, Mapping
from itertools import islice

import sys

# from .metaclass import datt
# from ._reshape import reshape

from .dobject import dobject
from .typing import DObject, DSet, DSetBase, DAttribute, AnyDObject
from .typing import parse_attr_value_many, consume_kwargs
from .metaclass import _make_pkey_class, DObjectMetaClass

from itertools import chain as iter_chain


_dset_class_tmpl = """\
class {typename}(DSetBase):
    __dobject_key__ = dobject_key
"""
def dset(*args, **kwargs):

    if len(args) != 1:
        errmsg = "dset(item_type, ...), item_type is required"
        raise ValueError(errmsg)

    item_type = args[0]
    if not issubclass(item_type, (DObject, DSetBase)):
        err = "The item of a dset must be a dobject or dset, not class '%s'"
        err %= item_type.__name__
        raise TypeError(err)

    if not item_type.__dobject_key__ :
        err = "The key of item type '%s' is required" % item_type.__name__
        raise TypeError(err)


    dominion_class = consume_kwargs(kwargs, '_dominion', (DObject, DSetBase))
    type_name = consume_kwargs(kwargs, '_name', (str,))


    undefined_attrs = set()
    key_attrs = OrderedDict()
    arg_name = '_key'
    if arg_name in kwargs:
        arg_value = parse_attr_value_many(kwargs.pop(arg_name), arg_name)
        for attr_name, attr in arg_value.items():
            if attr is None:
                undefined_attrs.add(attr_name)
                continue

            key_attrs[attr_name] = attr.copy()

            if attr.owner_class is not None:
                if dominion_class is None:
                    dominion_class = attr.owner_class
                else:
                    if not issubclass(dominion_class, attr.owner_class):
                        err = ("Type Conflicts. The dominion class "
                               "'%s' (at %d) and "
                               " %s (at %d) that defines attribute %s are "
                               "inconstinst")
                        err %= (dominion_class.__name__,
                                id(dominion_class),
                                attr.owner_class.__name__,
                                id(attr.owner_class),
                                attr_name)
                        raise ValueError(err)

    if not key_attrs and dominion_class: #
        for attr_name, attr in dominion_class.__dobject_key__.items():
            key_attrs[attr_name] = attr.copy()

    if dominion_class and undefined_attrs:
        # try find attribute definition from dominion class
        for attr_name in list(undefined_attrs):
            attr = dominion_class.__dobject_key__[attr_name]
            key_attrs[attr_name] = attr.copy()
            undefined_attrs.remove(attr_name)

    if undefined_attrs:
        errmsg = "There are undefined attributes: "
        errmsg += ', '.join(undefined_attrs)
        raise ValueError(errmsg)

    for attr_name in key_attrs :
        attr = key_attrs[attr_name]

    links = OrderedDict()
    for master_attr, slave_attr in kwargs.items():
        if isinstance(slave_attr, str):
            links[slave_attr] = master_attr

        elif isinstance(slave_attr, DAttribute):
            links[slave_attr.name] = master_attr

        else:
            errmsg = "The value of link '%s' must be an attribue object"
            errmsg %= master_attr
            raise ValueError(errmsg)


    if not key_attrs and dominion_class:
        key_attrs = dominion_class.__dobject_key__

    # ----------------------------------------------------------------------

    type_name = item_type.__name__

    if type_name is None:
        type_name = item_type.__name__
        if not type_name.endswith('_dset'):
            type_name += '_dset'

    class_code = "class {name}(DSetBaseImpl):\n".format(name = type_name)

    for attr_name in key_attrs:
        class_code += ' ' * 4
        class_code += "{name} = key_attrs['{name}']\n".format(name=attr_name)

    class_code += "    __dobject_key__ = list(key_attrs.keys())"

    #------------------------------------------------------------------------

    namespace = dict(DSetBaseImpl = DSetBaseImpl, key_attrs = key_attrs)

    exec(class_code, namespace)
    dset_cls = namespace[type_name]

    for attr_name, attr in key_attrs.items():
        setattr(dset_cls, attr_name, attr)

    dset_cls.__dset_item_class__ = item_type
    dset_cls.__dset_links__ = links
    dset_cls.__dominion_class__ = dominion_class

    try:
        frame = sys._getframe(1)
        dset_cls.__module__ = frame.f_globals.get('__name__', '__main__')
    except (AttributeError, ValueError):
        pass

    return dset_cls

class DSetBaseImpl(DSetBase, dobject):
    """The set of dobjects.
    """

    def __new__(cls, *args, **kwargs):

        dominion_obj = None
        arg_name = "_dominion"
        if arg_name in kwargs:
            dominion_obj = kwargs.pop(arg_name)
            if not isinstance(dominion_obj, DObject):
                errmsg = "The dominion object should be a dobject, not ''"
                errmsg %= (dominion_obj.__class__.__module__
                                        + dominion_obj.__class__.__name__)
                raise ValueError(errmsg)

        item_iterable = None
        origin_obj = None
        if args:
            if len(args) != 1:
                raise ValueError('Only one positional argument is required')

            if isinstance(args[0], DSetBase):
                origin_obj = item_iterable = args[0]
            elif isinstance(args[0], Iterable):
                item_iterable = args[0]
            else:
                err = ("The 1st positional argument must be dset or "
                       "iterable of %s")
                err %= self.__dset_item_type__.__name__
                raise TypeError(err)

        if dominion_obj is not None:
            if isinstance(dominion_obj, cls.__dominion_class__):
                orig_domi_obj = dominion_obj
            else:
                errmsg = "The type dominion should be a object of %s, not %s"
                errmsg %= (cls.__dominion_class__,
                            dominion_obj.__class__ .__module__ + '.' +
                            dominion_obj.__class__.__name__)
                raise TypeError(errmsg)

        elif origin_obj is not None:
            orig_domi_obj = origin_obj

        else:
            orig_domi_obj = None


        if orig_domi_obj is not None:
            domi_cls = cls.__dominion_class__
            if isinstance(orig_domi_obj, Mapping):
                for attr_name, attr in cls.__dobject_key__.items():
                    if attr_name in kwargs:
                        continue

                    if not hasattr(orig_domi_obj, attr_name):
                        continue

                    kwargs[attr_name] = orig_domi_obj[attr_name]
            else:
                for attr_name, attr in cls.__dobject_key__.items():
                    if attr_name in kwargs:
                        continue

                    if not hasattr(orig_domi_obj, attr_name):
                        continue

                    kwargs[attr_name] = getattr(orig_domi_obj, attr_name)

        for attr_name in kwargs:
            if attr_name not in cls.__dobject_key__:
                err = "Given attribute %s is unkown or not the key attribute"
                err += " in dset"
                err %= (attr_name)
                raise ValueError(err)

        instance = super(DSetBase, cls).__new__(cls, **kwargs)

        instance_setter = super(dobject, instance).__setattr__
        instance_setter('__dset_item_dict__',  OrderedDict())
        instance_setter('__dominion_object__',  dominion_obj)

        if item_iterable is not None:
            for item in item_iterable:
                instance._add(item)

        return instance

    def _item_value_subst(self):
        dset_cls = self.__class__
        item_cls = self.__dset_item_class__

        subst_mapping = getattr(dset_cls, '_dobject_subst_mapping', None)

        if subst_mapping is not None:
            return subst_mapping

        seen = set()
        subst_mapping = [] # [(item_attr_name, set_attr_name)]
        for item_attr_n, dset_attr_n in dset_cls.__dset_links__.items():
            subst_mapping.append((item_attr_n, dset_attr_n))
            seen.add(item_attr_n)
            seen.add(dset_attr_n) # avoid copying with dset_attr_n

        for attr_name in self.__class__.__dobject_key__:
            if attr_name in seen:
                continue

            if (attr_name not in item_cls.__dobject_key__ and
                    attr_name not in item_cls.__dobject_att__):
                continue

            subst_mapping.append((attr_name, attr_name))


        subst_values = dict((item_attr_name, getattr(self, dset_attr_name))
                                for item_attr_name, dset_attr_name
                                    in subst_mapping)

        setattr(dset_cls, '_dobject_subst_mapping', subst_values)

        return subst_values

    def _index_key(self, obj):
        """get key of a object or mapping"""

        if isinstance(obj, int):
            indice = self.__dset_item_dict__.keys()
            index = next(islice(indice, obj, obj + 1), None)

            if index is None:
                raise IndexError('no found index: ' + str(obj))
                return self.__dset_item_class__()

            return index

        key_cls = self.__dset_item_class__.__dobject_key_class__
        value_subst = self._item_value_subst()

        if isinstance(obj, DObject):
            values = []
            for attr_name in key_cls._attr_names:
                if attr_name in value_subst:
                    values.append(value_subst[attr_name])
                else:
                    values.append(getattr(obj, attr_name))

            return key_cls(tuple(values))

        elif isinstance(obj, Mapping):
            values = []
            for attr_name in key_cls._attr_names:
                if attr_name in value_subst:
                    values.append(value_subst[attr_name])
                else:
                    values.append(obj[attr_name])

            return key_cls(tuple(values))

        else:
            raise ValueError()

    def _add(self, obj):
        """
        If the identity of obj has been added, replace the old one with it.
        """

        item_cls = self.__dset_item_class__
        dset_cls = self.__class__

        subst_values = self._item_value_subst()

        # ------------------------------------------------------------

        obj = item_cls(obj, **subst_values) # clone it and replace its values

        self.__dset_item_dict__[obj.__dobject_key__] = obj

        return self

    def _clear(self):
        """clear all objects in aggregate"""

        self.__dset_item_dict__.clear()

        return self


    def __json_object__(self):
        """export dset object in list"""

        return [item.__json_object__()
                    for item in self.__dset_item_dict__.values()]

    def __bool__(self):
        return bool(self.__dset_item_dict__)

    def __len__(self):
        return len(self.__dset_item_dict__)

    def __iter__(self):
        for item in self.__dset_item_dict__.values():
            yield item

    def __repr__(self):

        opts = []

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

        index = self._index_key(index)

        obj = self.__dset_item_dict__.get(index, None)
        if obj is not None:
            return obj

        return self.__dset_item_class__()

    def __delitem__(self, index):

        index = self._index_key(index)
        del self.__dset_item_dict__[index]


    def __setitem__(self, index, value):

        item_class = self.__dset_item_class__

        index = self._index_key(index)
        self.__dset_item_dict__[index] = item_class(value)


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
