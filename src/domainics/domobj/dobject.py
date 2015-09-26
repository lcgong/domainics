# -*- coding: utf-8 -*-

import datetime as dt
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from decimal import Decimal
from itertools import chain as iter_chain

from .typing import DSet, DObject
from .metaclass import DObjectMetaClass
# from ._reshape import ReshapeOperator


class dobject(DObject, metaclass=DObjectMetaClass):

    def __new__(cls, *args, **kwargs):

        instance = super(dobject, cls).__new__(cls)  # new instance of dobject

        # store values of attributes
        super(dobject, instance).__setattr__('__value_dict__', OrderedDict())

        attributes = OrderedDict(iter_chain(cls.__primary_key__.items(),
                                            cls.__value_attrs__.items()))

        seen = set()
        if args:
            if len(args) > 1:
                errmsg = "Do not exceed one positional argument: "
                errmsg += "(obj, attr1='', ...) or (attr1='', ...) "
                raise ValueError(errmsg)

            source_obj = args[0] # reshape the given object or dict
            if isinstance(source_obj, Mapping): # like {}
                for attr_name, attr in attributes.items():
                    if attr_name in kwargs:
                        continue # this value will be set laterly

                    if attr_name not in source_obj:
                        continue

                    attr_val = source_obj[attr_name]
                    attr.set_value_unguardedly(instance, attr_val)
                    seen.add(attr_name)

            else:
                for attr_name, attr in attributes.items():
                    if attr_name in kwargs:
                        continue # this value will be set laterly

                    if not hasattr(source_obj, attr_name):
                        continue

                    attr_val = getattr(source_obj, attr_name)
                    attr.set_value_unguardedly(instance, attr_val)
                    seen.add(attr_name)

        for arg_name, arg_value in kwargs.items():

            attr = attributes.get(arg_name, None)
            if attr is None:
                errmsg = "No attribue '%s' defined in %s"
                errmsg %= (arg_name, cls.__name__)
                raise ValueError(errmsg)

            attr.set_value_unguardedly(instance, arg_value)
            seen.add(arg_name)

        # # set default values for these left parameters
        # for attr_name, attr in parameters.items():
        #     getattr(instance, attr_name)
        #     # force it to get chance to check default value

        pkey_att_vals = tuple(getattr(instance, attr_name)
                                for attr_name in cls.__primary_key__)

        setattr(instance, '__primary_key__',
                                        cls.__primary_key_class__(instance))

        return instance


    def __getattr__(self, name):
        errmsg ='The domain object %s has no field: %s '
        errmsg %= (self.__class__.__name__, name)
        raise AttributeError(errmsg)

    def __setattr__(self, name, value):
        if hasattr(self, name):
            super(dobject, self).__setattr__(name, value)
        else:
            errmsg ='The domain object %s has no field: %s '
            errmsg %= (self.__class__.__name__, name)
            raise AttributeError(errmsg)

    def __repr__(self):
        """ """
        values =  self.__value_dict__

        segs = [repr(self.__primary_key__)] if self.__primary_key__ else []
        segs += ['%s=%r' % (attr_name, values.get(attr_name))
                    for attr_name in self.__class__.__value_attrs__]

        return self.__class__.__name__ + '(' + ', '.join(segs) + ')'

    def __eq__(self, other) :
        ''' ducking equals:
        1. True if the identity of this object equals that of other;
        2. True if all of fields of this object equal those of other '''

        if other is None :
            return False

        this_id = self.__primary_key__
        if this_id is not None and this_id == other.__primary_key__:
            return True

        if set(self.__value_dict__.keys()) != set(other.__value_dict__.keys()):
            return False

        for name, val in self.__value_dict__.items():
            if val != other.__value_dict__[name]:
                return False

        return True

    def copy(self):
        self_attrs = getattr(self,   '__value_dict__')
        kwargs = OrderedDict()
        for attr_name in self_attrs:
            value = self_attrs[attr_name]

            if isinstance(value, DSet) or isinstance(value, dobject):
                value = value.copy()
            else: # some copy
                pass

            kwargs[attr_name] = value

        return self.__class__(**kwargs)

    def export(self):
        """export dobject as list or dict """

        self_attrs = getattr(self,   '__value_dict__')
        kwargs = OrderedDict()
        for attr_name in self_attrs:
            value = self_attrs[attr_name]

            if isinstance(value, DSet) or isinstance(value, dobject):
                value = value.export()
            else: # some copy
                pass

            kwargs[attr_name] = value

        return kwargs
