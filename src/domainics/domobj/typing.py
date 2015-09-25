# -*- coding: utf-8 -*-

from typing import TypeVar, Generic
from datetime import datetime, date
import arrow



class DObject:
    pass

AnyDObject = TypeVar('AnyDObject', bound=DObject, covariant=True)

class DSet(Generic[AnyDObject]):
    pass

class DAttribute:
    pass

class DAggregate(DAttribute):
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
