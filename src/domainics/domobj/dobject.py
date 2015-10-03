# -*- coding: utf-8 -*-

import datetime as dt
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from decimal import Decimal
from itertools import chain as iter_chain

from .typing import DSet, DObject, DSetBase
from .metaclass import DObjectMetaClass
# from ._reshape import ReshapeOperator


class dobject(DObject, metaclass=DObjectMetaClass):

    def __new__(cls, *args, **kwargs):

        instance = super(dobject, cls).__new__(cls)  # new instance of dobject


        # store values of attributes
        super(dobject, instance).__setattr__('__value_dict__', OrderedDict())

        attributes = OrderedDict(iter_chain(cls.__dobject_key__.items(),
                                            cls.__dobject_att__.items()))

        aggregates = []
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

            elif isinstance(source_obj, DObject):
                if (cls.__dobject_origin_class__ and
                        isinstance(source_obj, cls.__dobject_origin_class__)):
                    subst_mapping = {}
                    for o_name, n_name in cls.__dobject_mapping__.items():
                        subst_mapping[n_name] = o_name
                        if n_name not in cls.__dobject_mapping__:
                            # _subst=dict(a=b*, b=a) if o_name in mapping
                            subst_mapping[o_name] = None # mark it not to clone

                else:
                    subst_mapping = {}

                for attr_name, attr in attributes.items():
                    if attr_name in kwargs:
                        continue # this value will be set laterly

                    if attr_name in subst_mapping:
                        src_attr_name = subst_mapping[attr_name]
                        if src_attr_name is None:
                            continue
                    else:
                        src_attr_name = attr_name

                    if not hasattr(source_obj, src_attr_name):
                        continue

                    attr_val = getattr(source_obj, src_attr_name)
                    if isinstance(attr_val, DSetBase):
                        # NOTED: the dominion object is required to replace
                        aggregates.append((attr_name, attr, attr_val))
                        continue


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

        for attr_name, attr, attr_val in aggregates:
            attr_val = attr.type(attr_val, _dominion = instance)

        # # set default values for these left parameters
        # for attr_name, attr in parameters.items():
        #     getattr(instance, attr_name)
        #     # force it to get chance to check default value

        pkey_att_vals = tuple(getattr(instance, attr_name)
                                for attr_name in cls.__dobject_key__)

        setattr(instance, '__dobject_key__',
                                        cls.__dobject_key_class__(instance))

        return instance


    # def __getattr__(self, name):
    #     errmsg ='The domain object %s has no field: %s '
    #     errmsg %= (self.__class__.__name__, name)
    #     raise AttributeError(errmsg)

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

        segs = [repr(self.__dobject_key__)] if self.__dobject_key__ else []
        segs += ['%s=%r' % (attr_name, getattr(self, attr_name))
                    for attr_name in self.__class__.__dobject_att__]

        return self.__class__.__name__ + '(' + ', '.join(segs) + ')'

    def __eq__(self, other) :
        """
        When the primary key attribute is specified, this dobject is equal to
        the other if the attribues of primary key are equaled. Otherwise, all
        attributes are needed to be equaled if the two dobject are equaled.
        """

        if other is None :
            return False

        # if not isinstance(other, dobject):  # it's weird:   A() == 9999
        #
        if not isinstance(other, dobject):  #
            errmsg = "The value should be a dobject, not '%s' type"
            errmsg %= other.__class__.__name__
            raise ValueError(errmsg)
            # other = self.__class__(other) # it's weird:   A() == 9999

        if self.__class__.__dobject_key__:
            return self.__dobject_key__ == other.__dobject_key__

        for attr_name in self.__class__.__dobject_att__.keys():
            if getattr(self, attr_name) != getattr(other, attr_name, None):
                return False

        return True

    def __bool__(self):
        """
        """
        cls = self.__class__

        if not cls.__dobject_att__ and not cls.__dobject_key__:
            return False  # no attribues defined in this dobject

        for attr_name, attr in iter_chain(cls.__dobject_key__.items(),
                                          cls.__dobject_att__.items()):

            if attr_name not in self.__value_dict__:
                continue # The truth value of attribute is false

            attr_val = getattr(self, attr_name)
            if attr.default is not None:
                if attr_val != attr.default:
                    return True
            elif attr_val:
                    return True

        return False


    # def _copy(self):
    #     self_attrs = getattr(self,   '__value_dict__')
    #     kwargs = OrderedDict()
    #     for attr_name in self_attrs:
    #         value = self_attrs[attr_name]
    #
    #         if isinstance(value, DSet) or isinstance(value, dobject):
    #             value = value.copy()
    #         else: # some copy
    #             pass
    #
    #         kwargs[attr_name] = value
    #
    #     return self.__class__(**kwargs)

    def _export(self):
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
