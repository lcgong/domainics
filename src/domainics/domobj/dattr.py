# -*- coding: utf-8 -*-


from collections import OrderedDict
from collections import namedtuple
from collections.abc import Iterable
from typing import Mapping, Generic

from datetime import datetime, date, timedelta, time
from decimal import Decimal
# from dateutil.parser import parse as datetime_parse

from .typing import DSet, cast_attr_value, DAttribute, DAggregate
from .typing import DObject, AnyDObject, DSetBase
from .dset import dset

class datt(DAttribute):
    """Attribute of dobject"""

    __slots__ = ('name', 'type', 'default_expr', 'default', 'doc')

    def __init__(self, type=object, default=None, doc=None, **kwargs):
        self.name         = None
        self.type         = type
        self.default      = default
        self.doc          = doc
        self.owner_class  = None

        self._kwargs = kwargs


    def __get__(self, instance, owner):
        if instance is None: # get domain field
            return self

        attr_value = instance.__value_dict__.get(self.name, None)

        if attr_value is None and self.default is not None:
            attr_value = self.initialize(instance)
            self.set_value_unguardedly(instance, attr_value)

        return attr_value

    def __set__(self, instance, value):
        if self.name in instance.__class__.__dobject_key__:
            errmsg = "The primary key attribute '%s' is read-only"
            errmsg %= self.name
            raise ValueError(errmsg)

        self.set_value_unguardedly(instance, value)

    def setup(self, owner_class, attr_name):
        """
        When building the dobject class, it' required to setup this attribute
        to it (owner dobject class).
        """

        self.name = attr_name
        self.owner_class = owner_class

        if issubclass(self.type, DSetBase):
            dset_cls = self.type
            key_names = list(dset_cls.__dobject_key__.keys())

            self.type = dset(dset_cls.__dset_item_class__,
                             _dominion = owner_class,
                             _key = key_names
                             )

            self.type.__dset_links__ = dset_cls.__dset_links__

            self.default = self.type # the inializer of dset

    def initialize(self, instance):
        """  """

        if issubclass(self.type, DSetBase):
            return self.type(_dominion=instance)

        if isinstance(self.default, type):
            return self.default()

        if isinstance(self.default, (str, int, float, Decimal,
                                       date, time, datetime,
                                       timedelta)):
            return self.default

        errmsg = "Unknown the default value: %r" % self.default
        raise ValueError(errmsg)


    def set_value_unguardedly(self, instance, value):

        attr_values  = instance.__value_dict__
        if hasattr(self.type, '__setter_filter__'):
            value = self.type.__setter_filter__(value)
        else:
            value = cast_attr_value(self.name, value, self.type)

        attr_values[self.name] = value

    def copy(self):

        obj = datt(type=self.type, default=self.default, doc=self.doc,
                                **self._kwargs)
        obj.owner_class  = self.owner_class
        return obj
