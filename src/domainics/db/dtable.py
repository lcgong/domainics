# -*- coding: utf-8 -*-


from ..domobj import dobject, dset, DSetBase, datt


class dtable(dobject):
    pass

class tcol(datt):

    def __init__(self, datatype, len=None, nullable=True, **kwargs):
        self.len = len

        if issubclass(datatype, dsequence):
            if kwargs.get('default') is not None or not nullable:
                kwargs['default'] = datatype

        super(tcol, self).__init__(datatype, **kwargs)


class DBArray(DSetBase):

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

        if issubclass(value.__class__, (list, dict)):
            return value

        raise ValueError("the assigned value type should be one of"
                         " 'list', 'dict'")


class dsequence:
    """"""

    def __init__(self, value=None):
        if value is None or isinstance(value, int):
            self.__value = value
        elif isinstance(value, str):
            self.__value = int(value)
        else:
            err = 'dsequence should be integer, not %s'
            err %= value.__class__.__name__
            raise TypeError(err)

    @property
    def value(self):
        if self.__value is None:
            err = 'The sequence number %s is not allocated'
            err %= self.__class__.__name__
            raise ValueError(err)

        return self.__value

    @value.setter
    def value(self, newval):
        if isinstance(newval, int):
            self.__value = newval
        else:
            err = 'The sequence value should be int, not %s'
            err %= newval.__class__.__name__
            raise TypeError(err)

    def __bool__(self):
        return self.__value is not None

    def __int__(self):
        return self.value

    def __hash__(self):
        return super(dsequence, self).__hash__()

    def __repr__(self):
        if self.__value is not None:
            return repr(self.__value)
        else:
            return self.__class__.__name__ + '(' + ')'
