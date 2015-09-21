# -*- coding: utf-8 -*-

from itertools import chain as iter_chain
from collections import OrderedDict

import datetime as dt
from decimal import Decimal

from ..util import NamedDict

from collections.abc import Iterable, Mapping

from .metaclass import DObjectMetaClass, datt, daggregate, AggregateAttr, DObject

from ._reshape import ReshapeOperator, reshape

class dobject(DObject, metaclass=DObjectMetaClass):

    def __new__(cls, *values, **kwvalues):
        """
        ordinal value: in defined order and mro
        """
        values = list(values)
        instance = super(dobject, cls).__new__(cls)
        attr_values = OrderedDict()
        instance_setattr = super(dobject, instance).__setattr__
        instance_setattr('__value_dict__', attr_values)

        # if not values and not kwvalues:  # empty object
        #     return instance

        if values and isinstance(values[0], ReshapeOperator): # have a peek
            # this argument value is reshape operator, consume it
            values.pop().reshape_object(instance)

        parameters = OrderedDict(iter_chain(cls.__primary_key__.items(),
                                            cls.__value_attrs__.items()))

        seen = set()
        # consume parameters with the positional arguments
        for arg_value in values:
            if not parameters:
                raise TypeError('Too much positional argument given')

            arg_name, attr = parameters.popitem(last=False)

            attr.set_value_unguardedly(instance, arg_value)
            seen.add(arg_name)

        # consume parameters with the keyword arguments
        for arg_name, arg_value in kwvalues.items():
            if arg_name in seen:
                errmsg = "%s() got multiple values for argument '%s'"
                errmsg %= (cls.__name__, arg_name)
                raise TypeError(errmsg)

            attr = parameters.pop(arg_name, None)
            if attr is None:
                errmsg = "%s() got an unexpected keyword argument '%s'"
                errmsg %= (cls.__name__, arg_name)
                raise TypeError(errmsg)

            attr.set_value_unguardedly(instance, arg_value)

        # set default values for these left parameters
        for attr_name, attr in parameters.items():
            getattr(instance, attr_name)
            # force it to get chance to check default value

        pkey_att_vals = tuple(getattr(instance, attr_name)
                                for attr_name in cls.__primary_key__)
        pkey_obj = cls.__primary_key_class__(*pkey_att_vals)
        setattr(instance, '__primary_key__', pkey_obj)


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

            if isinstance(value, daggregate) or isinstance(value, dobject):
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

            if isinstance(value, daggregate) or isinstance(value, dobject):
                value = value.export()
            else: # some copy
                pass

            kwargs[attr_name] = value

        return kwargs

    def __ilshift__(self, target):
        """ x <<= y """

        self_attrs = getattr(self,   '_dobject_attrs')
        targ_attrs = getattr(target, '_dobject_attrs')

        for attr_name in self_attrs:
            if attr_name not in targ_attrs:
                continue

            ths_val = self_attrs[attr_name]
            tgt_val = targ_attrs[attr_name]
            if isinstance(ths_val, daggregate) or isinstance(ths_val, dobject):
                ths_val.__ilshift__(tgt_val)
                continue

            if ths_val == tgt_val:
                continue

            self_attrs[attr_name] = tgt_val

        return self

    # def _attrs_filtered(self, onlywith=None, ignore=None):
    #
    #     if not isinstance(onlywith, Iterable):
    #         if onlywith is None:
    #             onlywith = set()
    #         elif isinstance(onlywith, (datt, str)):
    #             onlywith = set([onlywith])
    #         else:
    #             raise ValueError("The argument 'onlywith' "
    #                              "should be a datt or str object")
    #     else:
    #         onlywith = set(onlywith)
    #
    #     if not isinstance(ignore, Iterable):
    #         if ignore is None:
    #             ignore = set()
    #         elif isinstance(ignore, (datt, str)):
    #             ignore = set([ignore])
    #         else:
    #             raise ValueError("The argument 'ignore' "
    #                              "should be a datt or str object")
    #     else:
    #         ignore = set(ignore)
    #
    #     pkey_attrs = []
    #     val_attrs = []
    #     if onlywith:
    #         for attr_name, attr in self.__class__.__primary_key__.items():
    #             if attr_name in ignore or attr in ignore:
    #                 continue
    #
    #             pkey_attrs.append(attr)
    #
    #         for attr_name, attr in self.__class__.__value_attrs__.items():
    #             if attr_name not in onlywith or attr not in onlywith:
    #                 continue
    #
    #             if attr_name in ignore or attr in ignore:
    #                 continue
    #
    #             val_attrs.append(attr)
    #
    #     else:
    #         for attr_name, attr in self.__class__.__primary_key__.items():
    #             if attr_name in ignore or attr in ignore:
    #                 continue
    #
    #             pkey_attrs.append(attr)
    #
    #         for attr_name, attr in self.__class__.__value_attrs__.items():
    #             if attr_name in ignore or attr in ignore:
    #                 continue
    #
    #             val_attrs.append(attr)
    #
    #     return val_attrs

    # def reform(self, other, onlywith=None, ignore=None, link=None):
    #     """
    #     Reform the dobject with src's attribues or fields.
    #
    #     A reformation of object means that the object is still the object
    #     itself. After reformation, primary key attributes are pristine.
    #
    #     The link is a dictionary defined with the connections between this
    #     attribute to some attribue of the other's.
    #
    #     Note: the onlywith does not applied on primary key attributes.
    #     """
    #
    #     self_val_attrs = self._attrs_filtered(onlywith, ignore)
    #
    #     NO_VALUE = object()
    #
    #     def copy_value(attr_name, self_val, other_val):
    #         if other_val is NO_VALUE:
    #             return
    #
    #         if (isinstance(self_val, daggregate) or
    #                 isinstance(self_val, dobject)):
    #
    #             self_val.reform(other_val)
    #             return
    #
    #         setattr(self, attr_name, other_val)
    #
    #     if isinstance(other, dict):
    #
    #         for attr in self_val_attrs:
    #             attr_name = attr.name
    #             self_val = getattr(self, attr_name)
    #             other_val = other.get(attr_name, NO_VALUE)
    #
    #             copy_value(attr_name, self_val, other_val)
    #
    #     elif isinstance(other, (dobject, NamedDict)):
    #         # support domobj and sqlblock
    #
    #         for attr in self_val_attrs:
    #             attr_name = attr.name
    #             self_val = getattr(self, attr_name)
    #             other_val = getattr(other,  attr_name, NO_VALUE)
    #
    #             copy_value(attr_name, self_val, other_val)
    #
    #     else:
    #         raise TypeError('Unknown type: ' + other.__class__.__name__)
    #
    #     return self
