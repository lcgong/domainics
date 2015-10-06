# -*- coding: utf-8 -*-


from collections import OrderedDict
from collections import namedtuple
from collections.abc import Iterable, Mapping
from typing import Mapping, Generic

from datetime import datetime, date
# from dateutil.parser import parse as datetime_parse

from .typing import DSet, DObject, PrimaryKeyTuple, AnyDObject
from .typing import DAttribute, DAggregate
from .typing import cast_attr_value, parse_attr_value_many
# from .dattr import AggregateAttr
from .reshape import ReshapeDescriptor


_keywords = set(["__module__", "__qualname__", "__new__", "__setattr__",
                 "__repr__", "__eq__", "__bool__", "__doc__", "__iter__",
                 "__getitem__", "__delitem__", "__setitem__", "__hash__",
                 "__iadd__", "__table_name__",
                 '_item_value_subst', '_index_key',
                 "_export", "_add", "_clear", "__json_object__", "__len__",
                 "__dobject_key__", "__dobject_att__", "__dobject_origin_class__", "__dobject_mapping__", "_re"])

class DObjectMetaClass(type):
    """A Metacalss of dobject.

    dobject classs

    attrs:
    :__dobject_key__: It is a attribute dictionary of primary key of domain object
    :__dobject_att__: It is a attribute dictionary of non primary key of domain object

    """

    @classmethod
    def __prepare__(metacls, name, bases, **kwargs):
        return OrderedDict()

    def __new__(metacls, classname, bases, class_dict, **kargs):

        pkey_attrs = OrderedDict()
        value_attrs = OrderedDict()
        for base_cls in reversed(bases): # overwriting priority, keep the first.
            attrs = getattr(base_cls, '__dobject_key__', None)
            if attrs is not None:
                pkey_attrs.update(attrs)

            attrs = getattr(base_cls, '__dobject_att__', None)
            if attrs is not None:
                value_attrs.update(attrs)

        attributes = OrderedDict()
        for attr_name, descriptor in class_dict.items():
            if attr_name.startswith('_'):
                if attr_name not in _keywords:
                    err = "Unknown preserved attribute in %s: %s"
                    err %= (classname, attr_name)
                    raise ValueError(err)

                continue

            if isinstance(descriptor, DAttribute):
                descriptor.name = attr_name
                attributes[attr_name] = descriptor
            else:
                err = "Unknown attribute declaration in %s: %s"
                err %= (classname, attr_name)
                raise TypeError(err)

        primary_key = class_dict.pop('__dobject_key__', None)
        primary_key = parse_attr_value_many(primary_key, '__dobject_key__')
        pkey_names = set(primary_key.keys())


        #----------------------------------------------------------------------

        if pkey_names:
            # If available, the primary key declaration overrides parent's
            for attr_name in tuple(reversed(pkey_attrs.keys())):
                if attr_name not in pkey_names:
                    # the pk attribute of child is not primary key
                    attr = pkey_names.pop(attr_name)
                    value_attrs[attr_name] = attr
                    value_attrs.move_to_end(attr_name, last=Fasle)


        for attr_name, attr in attributes.items():
            if attr.name in pkey_names:
                pkey_attrs[attr_name] = attr
            else:
                value_attrs[attr_name] = attr


        class_dict['__dobject_key__'] = pkey_attrs
        class_dict['__dobject_att__'] = value_attrs
        class_dict['__dobject_origin_class__'] = None
        class_dict['__dobject_mapping__'] = OrderedDict()
        class_dict['_re'] =  ReshapeDescriptor()

        cls = type.__new__(metacls, classname, bases, class_dict)

        # After class is created, ...
        # Becasuse the dset aggregate requires the complete class info.
        for attr_name, attr in attributes.items():
            attr.setup(cls, attr_name) # set owner and other something...

        setattr(cls, '__dobject_key_class__', _make_pkey_class(cls))

        return cls

    def __init__(cls, name, bases, namespace, **kargs):
        super().__init__(name, bases, namespace)

_pkey_class_tmpl = """\
class {typename}(PrimaryKeyTuple[DObjectType]):
    "Primary key value tuple"

    _attr_names = tuple([{attr_names}])

    def __init__(self, instance):
        if isinstance(instance, DObject):
            self._instance = instance
            self._attr_values = tuple(getattr(instance, n)
                                        for n in self._attr_names)
        elif isinstance(instance, tuple):
            self._attr_values = instance
        elif isinstance(instance, Mapping):
            self._attr_values = tuple(instance.get(n, None)
                                        for n in self._attr_names)
        else:
            errmsg = "The input value should be a dobject, tuple or dict object"
            errmsg += ": %s" % instance.__class__.__name__
            raise TypeError(errmsg)

    def __repr__(self):
        expr = ', '.join(['%s=%r' % (k, v)
                        for k, v in zip(self._attr_names, self._attr_values)])
        return 'K(' + expr + ')'

    def __eq__(self, other):
        return self._attr_values == other._attr_values

    def __hash__(self):
        return hash(self._attr_values)

    def __len__(self):
        return len(self._attr_names)

    def __iter__(self):
        return iter(self._attr_values)

    def as_dict(self):
        return dict((name, value)
                        for name, value
                            in zip(self._attr_names, self._attr_values))
"""
_pkey_attr_tmpl="""\
    {name} = property(lambda self: self._attr_values[{idx}])
"""

def _make_pkey_class(dobj_cls, attr_names = None):
    """ """

    typename = dobj_cls.__name__ + '_key_tuple'

    if attr_names is None:
        attr_names = dobj_cls.__dobject_key__

    attr_names = [repr(name) for name in attr_names]

    class_code = _pkey_class_tmpl.format(typename = typename,
                                         attr_names = ', '.join(attr_names))

    for i, attr_name in enumerate(dobj_cls.__dobject_key__):
        class_code += _pkey_attr_tmpl.format(name=attr_name, idx=i)

    namespace = dict(PrimaryKeyTuple = PrimaryKeyTuple,
                     DObjectType = dobj_cls,
                     DObject=DObject,
                     Mapping=Mapping)
    exec(class_code, namespace)
    pkey_cls = namespace[typename]
    pkey_cls.__module__ = dobj_cls.__module__

    return pkey_cls
