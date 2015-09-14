# -*- coding: utf-8 -*-


from collections import OrderedDict
from collections import namedtuple
from collections.abc import Iterable



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


        return getattr(instance, '__value_dict__').get(self.name)

    def __set__(self, instance, value):
        if self.name in instance.__class__.__primary_key__:
            raise ValueError("The primary key attribute '%s' is read-only" % name)

        self._unguarded_set_value(instance, value)

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

    __slots__ = ('name', 'agg_type', 'item_type', 'is_identity', 'doc')

    def __init__(self, agg_type, item_type, doc=None):
        self.agg_type     = agg_type
        self.item_type    = item_type
        self.is_identity  = False
        self.doc          = doc

    def __get__(self, instance, owner):
        if instance is None: # get domain attribute
            return self

        val = getattr(instance, '__value_dict__').get(self.name)
        return val

    def __set__(self, instance, value):
        oldval = getattr(instance, '__value_dict__').get(self.name)

        if oldval is value:
            # operator 'o.x += a', it is translated into o.x = o.x.__iadd__(a)
            return

        if not isinstance(value, self.agg_type):
            errmsg = "The aggregate object %s should be %s type, instead of %s"
            errmsg %= (self.name, 
                       self.agg_type.__name__, 
                       value.__class__.__name__)
            raise TypeError(errmsg)
        
        
        oldval.clear()
        for r in value:
            oldval.append(r)


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
            attrs = getattr(base_cls, '__primary_key__')
            if attrs is not None:
                pkey_attrs.update(attrs)

            attrs = getattr(base_cls, '__value_attrs__')
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
                                           descriptor.__doc__)

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
            pkey_attrs.clear()  # clear base class's primary key
        
        for attr in attributes:
            if attr.name in pkey_names:
                pkey_attrs[attr.name] = attr
            else:
                value_attrs[attr.name] = attr


        cls = type.__new__(metacls, classname, bases, class_dict)


        
        setattr(cls, '__primary_key_class__', namedtuple('PK', pkey_attrs.keys()))
        setattr(cls, '__primary_key__', pkey_attrs)
        setattr(cls, '__value_attrs__', value_attrs)

        return cls


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


