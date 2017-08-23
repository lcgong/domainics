# -*- coding: utf-8 -*-


from ..domobj import dobject, dset, DSetBase, datt


class dtable(dobject):
    pass

# class tcol(datt):
#
#     def __init__(self, datatype, len=None, nullable=True, **kwargs):
#         self.len = len
#
#         if issubclass(datatype, dsequence):
#             if kwargs.get('default') is not None or not nullable:
#                 kwargs['default'] = datatype
#
#         super(tcol, self).__init__(datatype, **kwargs)


class DBArray(object):

    @classmethod
    def __setter_filter__(cls, value):
        if value is None:
            return None

        if issubclass(value.__class__, (list,)):
            return value

        raise ValueError("the value should be list: %s" %
                                                value.__class__.__name__)


def array(item_type, iterable=None, dimensions=1, doc=None):
    cls_attrs = dict(dimensions=dimensions, item_type=item_type)
    cls_name = '_DBArray_%dD' % dimensions

    return type(cls_name, (DBArray,), cls_attrs)


class json_object(object):
    @classmethod
    def __setter_filter__(cls, value):
        if value is None:
            return None

        if issubclass(value.__class__, (list, dict, str)):
            return value

        raise ValueError("the assigned value type should be one of"
                         " 'list', 'dict'")

class dsequence:
    """"""
    def __init__(self, value = None):
        if isinstance(value, int):
            self.__value = value

        elif value is None:
            self.__value = value

        elif isinstance(value, str):
            self.__value = int(value)

        else:
            err = 'dsequence should be integer, not %s'
            err %= value.__class__.__name__
            raise TypeError(err)

    @classmethod
    def __default_value__(cls):
        return cls()

    @property
    def value(self):
        return self.__value

    @value.setter
    def value(self, newval):
        if isinstance(newval, int):
            self.__value = newval

        elif isinstance(newval, dsequence):
            self.__value = newval.__value

        elif isinstance(newval, str):
            self.__value = int(str)

        else:
            err = 'The sequence value should be int, not %s'
            err %= newval.__class__.__name__
            raise TypeError(err)

    @property
    def allocated(self):
        return self.__value is not None

    def __bool__(self):
        return self.__value is not None

    def __int__(self):
        return self.value

    def __json_object__(self):
        if self.__value is not None:
            return int(self)

        return None

    def __eq__(self, other):

        if isinstance(other, int):
            other_value = other
        elif isinstance(other, dsequence):
            other_value = other.__value
        else:
            return False
            # errmsg = 'required type: dsequence or int, not: '
            # errmsg += other.__class__.__name__
            # raise TypeError(errmsg)

        if self.__value is not None and other_value is not None:
            return self.__value == other_value;

        return id(self) == id(other)

    def __hash__(self):
        return super(dsequence, self).__hash__()

    def __repr__(self):
        if self.__value is not None:
            return repr(self.__value)
        else:
            return "<dsequence at 0x%x, unallocated>" % id(self)

    def __dobject_cast__(self, target_type):
        if issubclass(target_type, dsequence):
            return self

        elif issubclass(target_type, int):
            if self.__value is None:
                return None
            else:
                return self.__value

        else:
            raise TypeError("Unknown type: " + target_type.__name__)
