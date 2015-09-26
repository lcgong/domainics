# -*- coding: utf-8 -*-


from collections import OrderedDict
from collections import namedtuple
from collections.abc import Iterable
from typing import Mapping, Generic

from datetime import datetime, date, timedelta, time
from decimal import Decimal
# from dateutil.parser import parse as datetime_parse

from .typing import DSet, cast_attr_value, DAttribute, DAggregate


class datt(DAttribute):
    """Attribute of dobject"""

    __slots__ = ('name', 'type', 'default_expr', 'default', 'doc')

    def __init__(self, type=object, expr=None, default=None, doc=None):
        self.name         = None
        self.type         = type
        self.default_expr = expr
        self.default      = default
        self.doc          = doc
        # why not to define primary key in attribute
        # Such as, a pkey attribute is defined in parent, but not defined in child

    def __get__(self, instance, owner):
        if instance is None: # get domain field
            return self

        attr_value = instance.__value_dict__.get(self.name, None)

        if attr_value is None and self.default is not None:
            # set default value
            if isinstance(self.default, type):
                attr_value = self.default()
            elif isinstance(self.default, (str, int, float, Decimal,
                                           date, time, datetime,
                                           timedelta)):
                attr_value = self.default
            else:
                errmsg = "Unknown the default value: %r" % self.default
                raise ValueError(errmsg)

            self.set_value_unguardedly(instance, attr_value)

        return attr_value

    def __set__(self, instance, value):
        if self.name in instance.__class__.__dobject_key__:
            errmsg = "The primary key attribute '%s' is read-only"
            errmsg %= self.name
            raise ValueError(errmsg)

        self.set_value_unguardedly(instance, value)

    def set_value_unguardedly(self, instance, value):

        attr_values  = instance.__value_dict__
        if hasattr(self.type, '__setter_filter__'):
            value = self.type.__setter_filter__(value)
        else:
            value = cast_attr_value(self.name, value, self.type)

        attr_values[self.name] = value


class AggregateAttr(DAggregate):
    """The aggregate attribute of domain object"""

    __slots__ = ('name', 'agg_type', 'item_type', 'is_identity', 'doc',
                    '_item_primary_key', '_item_primary_key_class')

    def __init__(self, agg_type, item_type, doc=None,
                    primary_key=None, primary_key_class=None):

        self.agg_type = agg_type
        self.item_type = item_type
        self.is_identity = False
        self.doc = doc
        self._item_primary_key = primary_key
        self._item_primary_key_class = primary_key_class

    def __get__(self, instance, owner):
        if instance is None: # get attribute
            return self

        # get value of attribute
        value = instance.__value_dict__.get(self.name, None)
        if value is None:
            value = self.agg_type(item_type=self.item_type,
                                  primary_key=self._item_primary_key)
            instance.__value_dict__[self.name] = value

        return value

    def set_value_unguardedly(self, instance, value):

        dset_value = getattr(instance, self.name)
        if not isinstance(value, Iterable):
            errmsg = ("The assigned object (%s) to aggregate attribute "
                      "'%s' is not iterable")
            errmsg %= (value.__class__.__name__, self.name)
            raise TypeError(errmsg)

        dset_value.clear()
        for r in value:
            dset_value.add(r)

    __set__ = set_value_unguardedly
