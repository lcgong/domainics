# -*- coding: utf-8 -*-


from collections import OrderedDict
from collections import namedtuple
from collections.abc import Iterable
from typing import Mapping, Generic

from datetime import datetime, date
# from dateutil.parser import parse as datetime_parse

from .typing import DSet, DAttribute, DAggregate, PrimaryKeyTuple, AnyDObject
from .typing import cast_attr_value
from .dattr import AggregateAttr
from .reshape import ReshapeDescriptor



class DObjectMetaClass(type):
    """A Metacalss of dobject.

    dobject classs

    attrs:
    :__primary_key__: It is a attribute dictionary of primary key of domain object
    :__value_attrs__: It is a attribute dictionary of non primary key of domain object

    """

    @classmethod
    def __prepare__(metacls, name, bases, **kwargs):
        return OrderedDict()

    def __new__(metacls, classname, bases, class_dict):

        pkey_attrs = OrderedDict()
        value_attrs = OrderedDict()
        for base_cls in reversed(bases): # overwriting priority, keep the first.
            attrs = getattr(base_cls, '__primary_key__', None)
            if attrs is not None:
                pkey_attrs.update(attrs)

            attrs = getattr(base_cls, '__value_attrs__', None)
            if attrs is not None:
                value_attrs.update(attrs)

        primary_key = class_dict.pop('__primary_key__', None)

        attributes = OrderedDict()
        for attr_name, descriptor in class_dict.items():

            if isinstance(descriptor, DAttribute):
                pass
            elif isinstance(descriptor, DSet):
                # In class-block, dset object is used,
                # replace it with AggregateAttr
                descriptor = AggregateAttr(descriptor.__class__,
                                           descriptor.item_type,
                                           descriptor.__doc__,
                                           descriptor._item_primary_key,
                                           descriptor._item_primary_key_class)

                class_dict[attr_name] = descriptor
            else:
                continue

            descriptor.name = attr_name # give the attribute descriptor a name

            attributes[attr_name] = descriptor

        pkey_names = set()
        if primary_key:
            if isinstance(primary_key, DAttribute):
                pkey_names.add(primary_key.name)
            elif isinstance(primary_key, Iterable):
                for i, attr in enumerate(primary_key):
                    if isinstance(attr, DAttribute):
                        pkey_names.add(attr.name)
                    elif isinstance(attr, str):
                        attr = attributes.get(attr, None)
                        if attr is None:
                            errmsg = "No attribute names '%s'" % attr
                            raise TypeError(errmsg)
                        pkey_names.add(attr.name)
                    else:
                        errmsg = "The %d-th element in __primary_key__ should"
                        errmsg += "a attribute or attribute name"
                        errmsg %= i
                        raise TypeError(errmsg)
            else:
                raise TypeError('__primary_key__ should be a DAttribute object '
                                'or an iterable of DAttribute object')

        if pkey_names:
            # If available, the primary key declaration overrides parent's
            for attr_name in tuple(reversed(pkey_attrs.keys())):
                if attr_name not in pkey_names:
                    # the pk attribute of child is not primary key
                    attr = pkey_names.pop(attr_name)
                    value_attrs[attr_name] = attr
                    value_attrs.move_to_end(attr_name, last=Fasle)

        for attr in attributes.values():
            if attr.name in pkey_names:
                pkey_attrs[attr.name] = attr
            else:
                value_attrs[attr.name] = attr

        cls = type.__new__(metacls, classname, bases, class_dict)

        setattr(cls, '__primary_key__', pkey_attrs)
        setattr(cls, '__primary_key_class__', _make_pkey_tuple_class(cls))

        setattr(cls, '__value_attrs__', value_attrs)
        setattr(cls, '_re', ReshapeDescriptor())

        return cls



_pkey_class_tmpl = """\
class {typename}(PrimaryKeyTuple[DObjectType]):
    "Primary key value tuple"

    def __init__(self, instance):
        self._instance = instance
        attr_names = instance.__class__.__primary_key__.keys()
        self._attr_values = tuple(getattr(instance, n) for n in attr_names)

    def __repr__(self):
        attr_names = self._instance.__class__.__primary_key__.keys()
        expr = ', '.join(['%s=%r' % (k, v)
                            for k, v in zip(attr_names, self._attr_values)])
        return 'PK(' + expr + ')'

    def __eq__(self, other):
        return self._attr_values == other._attr_values

    def __hash__(self):
        return hash(self._attr_values)
"""
_pkey_attr_tmpl="""\
    {name} = property(lambda self: self._attr_values[{idx}])
"""

def _make_pkey_tuple_class(dobj_cls):
    """ """

    typename = dobj_cls.__name__ + '_pkey_tuple'

    class_code = _pkey_class_tmpl.format(typename=typename)
    for i, attr_name in enumerate(dobj_cls.__primary_key__):
        class_code += _pkey_attr_tmpl.format(name=attr_name, idx=i)

    namespace = dict(PrimaryKeyTuple=PrimaryKeyTuple, DObjectType=dobj_cls)
    exec(class_code, namespace)
    pkey_cls = namespace[typename]
    pkey_cls.__module__ = dobj_cls.__module__

    return pkey_cls
