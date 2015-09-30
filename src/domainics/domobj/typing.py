# -*- coding: utf-8 -*-

from typing import TypeVar, Generic
from datetime import datetime, date
import arrow
from collections import OrderedDict
from collections.abc import Iterable, Mapping


class DObject:
    pass

AnyDObject = TypeVar('AnyDObject', bound=DObject, covariant=True)

class DSet(Generic[AnyDObject]):
    pass

# it's workarond, because of the conflicts of metaclas
class DSetBase:
    pass

class DAttribute:
    pass

class DAggregate(DAttribute):
    pass

class PrimaryKeyTuple(Generic[AnyDObject]):
    pass

def cast_attr_value(attrname, val, attr_type):
    if val is None:
        return val

    if isinstance(val, attr_type):
        return val

    try:
        if isinstance(val, str) and issubclass(attr_type, (datetime, date)):
            val = arrow.get(val).datetime
            if issubclass(attr_type, date):
                return val.date()
            else:
                return val
        else:
            return attr_type(val)
    except (ValueError, TypeError) as ex:
        err = "The attribute '%s' should be \'%s\' type, not '%s'"
        err %= (attrname, attr_type.__name__, type(val).__name__)
        raise TypeError(err).with_traceback(ex.__traceback__)


# def pop_kwargs_attrs(arg_name, kwargs):
#     """
#     Parse keyword argument, get a attribues in OrderedDict,
#     {attr_name:attr_object}.
#
#     * (..., _arg = '', ...)
#     * (..., _arg = attr1, ...)
#     * (..., _arg = ['attr1', attr2, ...], ...)
#     * (..., _arg = ('attr1', attr2, ...), ...)
#
#     """
#
#     values = OrderedDict()
#     if arg_name not in kwargs:
#         return values

    arg_value = kwargs.pop(arg_name)

def parse_attr_value_many(arg_value, arg_name=None):
    values = OrderedDict()
    if arg_value is None:
        return values

    if isinstance(arg_value, str):
        values[arg_value] = None
    elif isinstance(arg_value, Mapping):
        for name, elem in arg_value.items():
            if isinstance(elem, str):
                values[name] = None
            elif isinstance(elem, DAttribute):
                values[name] = elem
            else:
                if arg_name is not None:
                    errmsg = ("The %d-th element of the argument '%s' "
                              "should be a str or DAttribute object: %s")
                    errmsg %= (elem, arg_name, arg_value.__class__.__name__)
                else:
                    errmsg = ("The value should be a str or "
                              "attribute object, not %s")
                    errmsg %= (arg_value.__class__.__name__)

                raise ValueError(errmsg)

    elif isinstance(arg_value, Iterable):
        for i, elem in enumerate(arg_value):
            if isinstance(elem, str):
                values[elem] = None
            elif isinstance(elem, DAttribute):
                values[elem.name] = elem
            else:
                if arg_name is not None:
                    errmsg = ("The %d-th element of the argument '%s' "
                              "should be a str or DAttribute object: %s")
                    errmsg %= (elem, arg_name, arg_value.__class__.__name__)
                else:
                    errmsg = ("The value should be a str or "
                              "attribute object, not %s")
                    errmsg %= (arg_value.__class__.__name__)

                raise ValueError(errmsg)

    elif isinstance(arg_value, DAttribute):
        if arg_value.name is None:
            err = "The name of attribue is None"
            raise ValueError(err)

        values[arg_value.name] = arg_value
    else:
        if arg_name is not None:
            errmsg = ("The value of %s should be an iterable object of "
                      "str or attribute object, not %s")
            errmsg %= (arg_name, arg_value.__class__.__name__)
        else:
            errmsg = ("The value should be an iterable object of "
                      "str or attribute object, not %s")
            errmsg %= arg_value.__class__.__name__

        raise ValueError(errmsg)

    return values


def consume_kwargs(kwargs, arg_name, required_types):
    if arg_name not in kwargs:
        return None

    arg_value = kwargs.pop(arg_name)
    if not issubclass(arg_value, required_types):
        errmsg = "Argument '%s' should be a object of %s, not %s"
        if len(required_types) > 1:
            typelist = ', '.join(required_types[:-1])
            typelist += 'or ' +  required_types[-1]
        errmsg %= (arg_name, typelist, arg_value.__class__.__name__)
        raise  ValueError(errmsg)

    return arg_value
