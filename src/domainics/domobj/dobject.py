# -*- coding: utf-8 -*-

from itertools import chain as iter_chain
from collections import OrderedDict

import datetime as dt
from decimal import Decimal

from ..util import NamedDict

from collections.abc import Iterable, Mapping

from .metaclass import DObjectMetaClass, datt, daggregate, AggregateAttr

"""
A(reform(dobj, a, b, ignore=[c]), k=1)

"""


class Transformation:
    __slot__ = ('source', 'requred', 'ignored')

    def __init__(self, source):
        self.source = source
        self.required = OrderedDict()
        self.ignored = OrderedDict()


def transform(source, *args, **kwargs):
    """Reforme the source dobject object.

    Attributes required
    reform(source_object, 'attr_name1', attr2, attr3=True)

    Attributes ignored:
    reform(source_object, ignored_attr1=False, ignored_attr2=False)
    reform(source_object, ignore=('ignored_attr1', ignored_attr2))
    """
    definition = Transformation(source)

    for i, arg in enumerate(args):
        if isinstance(arg, str):
            definition.required[arg] = True
        elif isinstance(arg, datt):
            definition.required[arg.name] = True
        else:
            errmsg = "The %dth argument should be a str or datt object: %r"
            errmsg %= (i + 1, arg)
            raise ValueError(errmsg)

    for arg, arg_value in kwargs.items():
        if arg == 'ignore' and isinstance(arg_value, Iterable):
            for i, elem in enumerate(arg_value):  # ignore=(attr1, 'attr2')
                if isinstance(elem, str):
                    definition.ignored[elem] = True
                elif isinstance(elem, datt):
                    definition.ignored[elem.name] = True
                else:
                    errmsg = ("The %d-th element in 'ignore' argument "
                              "should be a str or datt object: %r")
                    errmsg %= (elem, arg_value)
                    raise ValueError(errmsg)
        elif arg == 'ignore' and isinstance(arg_value, datt):
            definition.ignored[arg_value.name] = True

        elif isinstance(arg_value, bool):
            if arg_value:
                definition.required[arg] = True
            else:
                definition.ignored[arg] = True

        else:
            errmsg = ("The keyword argument(%s) should be "
                      "True or False, not: %r")
            errmsg %= (arg, arg_value)
            raise ValueError(errmsg)

    return definition


class dobject(metaclass=DObjectMetaClass):

    def _make_transformation(self, trans):
        """"""
        this_class = self.__class__

        selected_attrs = OrderedDict()
        if trans.required:
            for attr_name, attr in iter_chain(
                                        this_class.__primary_key__,items(),
                                        this_class.__value_attrs__.items()):

                if attr_name not in trans.requred:
                    continue

                if attr_name in trans.ignore:
                    continue
                selected_attrs[attr_name] = attr

        else:
            for attr_name, attr in iter_chain(
                                        this_class.__primary_key__.items(),
                                        this_class.__value_attrs__.items()):

                if attr_name not in trans.ignored:
                    selected_attrs[attr_name] = attr


        if isinstance(trans.source, dobject):
            for attr_name, attr in selected_attrs.items():
                if hasattr(trans.source, attr_name):
                    attr_val = getattr(trans.source, attr_name)
                    attr.set_value_unguardedly(self, attr_val)

        elif isinstance(trans.source, Mapping):
            for attr_name, attr in selected_attrs.items():
                if attr_name in trans.source:
                    attr.set_value_unguardedly(self, trans.source[attr_name])


    def __new__(cls, *values, **kwvalues):
        """
        ordinal value: in defined order and mro
        """
        values = list(values)
        instance = super(dobject, cls).__new__(cls)
        attr_values = OrderedDict()
        instance_setattr = super(dobject, instance).__setattr__
        instance_setattr('__value_dict__', attr_values)

        if not values and not kwvalues:  # empty object
            return instance

        if values and isinstance(values[0], Transformation):
            instance._make_transformation(values.pop()) # consume the argument

        parameters = OrderedDict(iter_chain(cls.__primary_key__.items(),
                                            cls.__value_attrs__.items()))
        print(parameters)

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
            getattr(instance, attr_name)  # force it to get chance to check default value

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

        segs = [repr(self.__primary_key__)]
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

    def _attrs_filtered(self, onlywith=None, ignore=None):

        if not isinstance(onlywith, Iterable):
            if onlywith is None:
                onlywith = set()
            elif isinstance(onlywith, (datt, str)):
                onlywith = set([onlywith])
            else:
                raise ValueError("The argument 'onlywith' "
                                 "should be a datt or str object")
        else:
            onlywith = set(onlywith)

        if not isinstance(ignore, Iterable):
            if ignore is None:
                ignore = set()
            elif isinstance(ignore, (datt, str)):
                ignore = set([ignore])
            else:
                raise ValueError("The argument 'ignore' "
                                 "should be a datt or str object")
        else:
            ignore = set(ignore)

        pkey_attrs = []
        val_attrs = []
        if onlywith:
            for attr_name, attr in self.__class__.__primary_key__.items():
                if attr_name in ignore or attr in ignore:
                    continue

                pkey_attrs.append(attr)

            for attr_name, attr in self.__class__.__value_attrs__.items():
                if attr_name not in onlywith or attr not in onlywith:
                    continue

                if attr_name in ignore or attr in ignore:
                    continue

                val_attrs.append(attr)

        else:
            for attr_name, attr in self.__class__.__primary_key__.items():
                if attr_name in ignore or attr in ignore:
                    continue

                pkey_attrs.append(attr)

            for attr_name, attr in self.__class__.__value_attrs__.items():
                if attr_name in ignore or attr in ignore:
                    continue

                val_attrs.append(attr)

        return val_attrs

    def reform(self, other, onlywith=None, ignore=None, link=None):
        """
        Reform the dobject with src's attribues or fields.

        A reformation of object means that the object is still the object
        itself. After reformation, primary key attributes are pristine.

        The link is a dictionary defined with the connections between this
        attribute to some attribue of the other's.

        Note: the onlywith does not applied on primary key attributes.
        """

        self_val_attrs = self._attrs_filtered(onlywith, ignore)

        NO_VALUE = object()

        def copy_value(attr_name, self_val, other_val):
            if other_val is NO_VALUE:
                return

            if (isinstance(self_val, daggregate) or
                    isinstance(self_val, dobject)):

                self_val.reform(other_val)
                return

            setattr(self, attr_name, other_val)

        if isinstance(other, dict):

            for attr in self_val_attrs:
                attr_name = attr.name
                self_val = getattr(self, attr_name)
                other_val = other.get(attr_name, NO_VALUE)

                copy_value(attr_name, self_val, other_val)

        elif isinstance(other, (dobject, NamedDict)):
            # support domobj and sqlblock

            for attr in self_val_attrs:
                attr_name = attr.name
                self_val = getattr(self, attr_name)
                other_val = getattr(other,  attr_name, NO_VALUE)

                copy_value(attr_name, self_val, other_val)

        else:
            raise TypeError('Unknown type: ' + other.__class__.__name__)

        return self
