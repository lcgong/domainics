# -*- coding: utf-8 -*-


from collections import OrderedDict
from collections import namedtuple
from collections.abc import Iterable
from typing import TypeVar, Mapping, Generic



# _primary_key_class_tmpl = """\
# class PK:
#     "Primary Key of dobject"

#     __slots__ = ('__value_dict__')

#     def __init__(self, value_dict):
#         self.__value_dict__ = value_dict

#     def __repr__(self):
#         expr = ','.join(['%s=%r' % (attr_name, getattr(self, attr_name))
#                          for attr_name in primary_key])

#         return self.__class__.__name__ + '(' + expr + ')'

#     def __tuple__(self):
#         return tuple(getattr(self, attr_name)
#                         for attr_name in primary_key)

#     def __eq__(self, other):
#         return tuple(self) == tuple(other)

# """

# _primary_key_prop_tmpl ="""\
#     {attr_name} = property(lambda self: self.__value_dict__.get('{attr_name}'))
# """

# def _make_pkey_class(cls, pkey_attrs):
#     """Make PrimaryKey class"""

#     properties = []
#     for attr_name in pkey_attrs:
#         prop = _primary_key_prop_tmpl.format(**dict(attr_name=attr_name))
#         properties.append(prop)

#     properties = '\n'.join(properties)

#     class_code = _primary_key_class_tmpl + '\n' + properties
#     namespace  = dict(primary_key=pkey_attrs)

#     exec(class_code, namespace)
#     cls = namespace['PK']

#     return cls

class datt:
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
                                           dt.date, dt.time, dt.datetime,
                                           dt.timedelta)):
                attr_value = self.default
            else:
                errmsg = "Unknown the default value: %r" % self.default
                raise ValueError(errmsg)

            self.set_value_unguardedly(instance, attr_value)

        return attr_value

    def __set__(self, instance, value):
        if self.name in instance.__class__.__primary_key__:
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


class daggregate:
    pass

class AggregateAttr:
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


class DObjectMetaClass(type):
    """A Metacalss of dobject.

    dobject classs

    attrs:
    :__primary_key__: It is a attribute dictionary of primary key of domain object
    :__value_attrs__: It is a attribute dictionary of non primary key of domain object

    """

    @classmethod
    def __prepare__(metacls, name, bases, **kwargs):
        return OrderedDict()

    def __new__(metacls, classname, bases, class_dict):

        pkey_attrs = OrderedDict()
        value_attrs = OrderedDict()
        for base_cls in reversed(bases): # overwriting priority, keep the first.
            attrs = getattr(base_cls, '__primary_key__', None)
            if attrs is not None:
                pkey_attrs.update(attrs)

            attrs = getattr(base_cls, '__value_attrs__', None)
            if attrs is not None:
                value_attrs.update(attrs)

        primary_key = class_dict.pop('__primary_key__', None)

        attributes = []
        for attr_name, descriptor in class_dict.items():

            if isinstance(descriptor, datt):
                pass
            elif isinstance(descriptor, daggregate):
                # In class-block, dset or daggregate object is used,
                # replace it with AggregateAttr
                descriptor = AggregateAttr(descriptor.__class__,
                                           descriptor.item_type,
                                           descriptor.__doc__,
                                           descriptor._item_primary_key,
                                           descriptor._item_primary_key_class)

                class_dict[attr_name] = descriptor
            else:
                continue

            descriptor.name = attr_name # give the attribute descriptor a name

            attributes.append(descriptor)

        pkey_names = set()
        if primary_key:
            if isinstance(primary_key, datt):
                pkey_names.add(primary_key.name)
            elif isinstance(primary_key, Iterable):
                for attr in primary_key:
                    pkey_names.add(attr.name)
            else:
                raise TypeError('__primary_key__ should be a datt object '
                                'or an iterable of datt object')

        if pkey_names:
            # If available, the primary key declaration overrides parent's
            for attr_name in tuple(reversed(pkey_attrs.keys())):
                if attr_name not in pkey_names:
                    # the pk attribute of child is not primary key
                    attr = pkey_names.pop(attr_name)
                    value_attrs[attr_name] = attr
                    value_attrs.move_to_end(attr_name, last=Fasle)

        for attr in attributes:
            if attr.name in pkey_names:
                pkey_attrs[attr.name] = attr
            else:
                value_attrs[attr.name] = attr

        cls = type.__new__(metacls, classname, bases, class_dict)

        setattr(cls, '__primary_key_class__',
                                        namedtuple('PK', pkey_attrs.keys()))
        setattr(cls, '__primary_key__', pkey_attrs)
        setattr(cls, '__value_attrs__', value_attrs)

        return cls

    # def __repr__(self):
    #     return 'A'

def cast_attr_value(attrname, val, attr_type):
    if val is None:
        return val

    if isinstance(val, attr_type):
        return val

    try:
        return attr_type(val)
    except (ValueError, TypeError) as ex:
        err = "The attribute '%s' should be \'%s\' type, not '%s'"
        err %= (attrname, attr_type.__name__, type(val).__name__)
        raise TypeError(err).with_traceback(ex.__traceback__)


class DObject():
    pass

T = TypeVar('T')

class DSet(Generic[T]):
    pass
