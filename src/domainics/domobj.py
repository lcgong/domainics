# -*- coding: utf-8 -*-


"""
Domain: 
A sphere of knowledge (ontology), influence, or activity. The subject area 
to which the user applies a program is the domain of the software.

Entity: 
An object that is not defined by its attributes, but rather by a thread of 
continuity and its identity.

Value Object: 
An object that contains attributes but has no conceptual identity. 
They should be treated as immutable.

Aggregate: 
A collection of objects that are bound together by a root entity, 
otherwise known as an aggregate root. 
The aggregate root guarantees the consistency of changes being made within the aggregate 
by forbidding external objects from holding references to its members.

"""

import logging

import sys
import inspect
from itertools import chain as iter_chain
from collections import OrderedDict
from collections import namedtuple

import datetime as dt
from decimal import Decimal

from .util import NamedDict

from collections.abc import Iterable

"""

    dobject classs
    attrs:
        :__primary_key__: It is a attribute dictionary of primary key of domain object
        :__value_attrs__: It is a attribute dictionary of non primary key of domain object

    dobject object

        :__primary_key__: It is a object of PrimaryKey that is a compound 
                          of attribute value as a primary key of the domain object. 
"""



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


class DObjectMetaClass(type):

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
            elif isinstance(descriptor, dset):
                # In class-block, dset is used, replace it with AggregateAttr
                descriptor = AggregateAttr(dset, descriptor.item_type, 
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


class datt:
    """ """

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
        name = self.name
        if name in instance.__class__.__primary_key__:
            raise ValueError("The primary key attribute '%s' is read-only" % name)

        attrs  = getattr(instance, '__value_dict__')
        if hasattr(self.type, '__setter_filter__'):
            value = self.type.__setter_filter__(value)
        else:
            value   = cast_attr_value(name, value, self.type)  
        
        attrs[name] = value



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

class daggregate:
    pass

class dset(daggregate):
    """
    The aggregate object set of domain object.
    """

    item_type = None

    def __init__(self, item_type, iterable=None, doc=None, primary_key=None):
        self.__list = []
        self.__map  = {}

        if not isinstance(item_type, type):
            raise TypeError('item_type should be a type object')

        self.item_type   = item_type

        if primary_key is not None:
            if isinstance(primary_key, datt):
                primary_key = OrderedDict([(primary_key.name, primary_key)])

            elif isinstance(primary_key, str):
                if primary_key in item_type.__primary_key__:
                    primary_key = OrderedDict([(primary_key, 
                                        item_type.__primary_key__[primary_key])])

                elif primary_key in item_type.__value_attrs__:
                    primary_key = OrderedDict([(primary_key, 
                                        item_type.__value_attrs__[primary_key])])
                else:
                    errmsg = "No '%s' attribute is defined in %s"
                    errmsg %= (primary_key, item_type.__name__)
                    raise ValueError(errmsg)
            
            elif isinstance(primary_key, Iterable):
                pkeys = OrderedDict()
                for attr in primary_key:
                    if isinstance(attr, datt):
                        pkeys[attr.name] = attr

                    elif isinstance(attr, str):
                        if attr in item_type.__primary_key__:
                            attr = item_type.__primary_key__[attr]

                        elif attr in item_type.__value_attrs__:
                            attr = item_type.__value_attrs__[attr]
                        else:
                            errmsg = "No '%s' attribute is defined in %s"
                            errmsg %= (attr, item_type.__name__)
                            raise ValueError(errmsg)
                        pkeys[attr.name] = attr
                    else:
                        raise ValueError('primary_key should be a datt, '
                                         'str object or a collection of it')
                primary_key = pkeys
            
            else:
                raise TypeError('primary_key should be a datt object ' 
                                'or an iterable of datt object')
            
            self._primary_key = primary_key
            self._primary_key_class = namedtuple('PK', (n for n in primary_key))

        else:
            if not item_type.__primary_key__:
                errmsg = "primary key should be given in primary_key argument "
                errmsg += " or be defined in %s class "
                errmsg %= item_type.__name__
                raise ValueError(errmsg)

            self._primary_key = item_type.__primary_key__
            self._primary_key_class = item_type.__primary_key_class__

        

        self.__attr_doc  = doc

        if iterable is not None:
            if hasattr(iterable, '__dset__'):
                dset_iter = getattr(iterable, '__dset__')
                for obj in dset_iter(item_type):
                    self.append(obj)
            else:
                for obj in iterable:
                    self.append(obj)

    
    def append(self, obj):
        """
        If the identity of obj has been appened, replace the old one with it. 
        """
        if not isinstance(obj, self.item_type):
            errmsg = "The aggregate object should be '%s' type" 
            errmsg %= self.item_type.__name__
            raise TypeError(errmsg)

        print(333, self._primary_key)
        pkey = self._primary_key_class(*tuple(getattr(obj, n) 
                                        for n in self._primary_key.keys()))
        # pkey = obj.__primary_key__
        if not pkey:
            errmsg = "The item's identity of %s is required" 
            errmsg %= obj.__class__.__name__
            raise TypeError(errmsg)
        
        if pkey in self.__map:
            index = self.__map[pkey]
            self.__list[index] = obj
        else:
            index = len(self.__list)
            self.__map[pkey] = index
            self.__list.append(obj)

    def clear(self):
        """clear all objects in aggregate"""

        self.__list.clear()
        self.__map.clear()

    def index(self, obj):
        """The index of the object in this aggregate"""

        if isinstance(obj, tuple):
            dobj_id = obj
        elif isinstance(obj, int):
            pass
        elif isinstance(obj, dobject):
            dobj_id = obj._dobj_id
        else:
            errmsg = 'The type of object should be dobject, identity or int: %s'
            errmsg %= obj.__class__.__name__
            raise TypeError(errmsg)

        index = self.__map.get(dobj_id)
        if index is None:
            raise ValueError ('no value of the identity %r' % dobj_id)
        return index

    def copy(self):
        """get a copy of the aggregate object and copies of its items"""

        items = (item.copy() for item in self.__list)
        return dset(self.item_type, items)

    def export(self):
        """export dset object in list"""

        return [item.export() for item in self.__list]

    def __ilshift__(self, target):
        """ x <<= y, the domain object x conforms to y """        
        for objid in list(self.__map):
            if objid not in target.__map: # need to be deleted items
                del self[objid]
                continue

            self[objid].__ilshift__(target[objid])

        for objid in target.__map:
            if objid in self.__map:
                continue

            # items that be inserted 
            newval = self.item_type(**dict([z for z in zip(objid._fields, objid)]) )
            newval.__ilshift__(target[objid])
            self.append(newval)

        return self
    
    def __bool__(self):
        return bool(self.__list)

    def __len__(self):
        return len(self.__list)

    def __iter__(self):
        for itemobj in self.__list:
            yield itemobj

    def __repr__(self):
        s = 'dset('
        s += ', '.join([repr(obj) for obj in self.__list])
        s += ')'
        return s


    def __getitem__(self, index):

        if isinstance(index, dobject) or isinstance(index, tuple):
            idx = self.index(index)
            return self.__list[idx]

        elif isinstance(index, int):
            return self.__list[index]
                    
        elif isinstance(index, slice):
            return dset(self.item_type, self.__list.__getitem__(index))

        else:
            errmsg = 'unknown index or slice %s(%r)'
            errmsg %= (index.__class__.__name__, index)
            raise TypeError(errmsg)

    def __delitem__(self, index):

        if isinstance(index, dobject) :
            idx = self.index(index)
            del self.__list[idx]
            del self.__map[index._dobj_id]
        
        elif isinstance(index, tuple):
            idx = self.index(index)
            del self.__list[idx]
            del self.__map[index]

        elif isinstance(index, int):
            item = self.__list[index]
            del self.__list[index]
            del self.__map[item._dobj_id]

        elif isinstance(index, slice):
            lst = [self.index(item) for item in self.__list.__getitem__(index)]
            for idx in sorted(lst, reverse=True): # delete 
                item = self.__list[idx]
                del self.__list[idx]
                del self.__map[item._dobj_id]
        else:
            errmsg = 'unknown index or slice %s(%r)'
            errmsg %= (index.__class__.__name__, index)
            raise TypeError(errmsg)

    def __setitem__(self, index, value):
        if not isinstance(value, dobject):
            raise TypeError('The assigned value should be dobject')

        if isinstance(index, int):
            # if the identity of this indexed object is different, 
            # rechange the identity with the new one.
            oldval = self.__list[index]
            
            newval_id = value._dobj_id
            oldval_id = oldval._dobj_id
            if oldval_id != newval_id:
                del self.__map[oldval_id]
                self.__map[newval_id] = index

            self.__list[index] = value

        elif isinstance(index, dobject):
            self.__list[self.index(index) ] = value
        elif isinstance(index, slice):
            raise NotImplementedError()
        else:
            errmsg = 'unknown index or slice %s(%r)'
            errmsg %= (index.__class__.__name__, index)
            raise TypeError(errmsg)


    def __eq__(self, other):
        if isinstance(other, dset):
            other_iter = other.__list
        elif isinstance(other, list) or isinstance(other, tuple):
            other_iter = other
        else:
            return False

        if len(self.__list) != len(other_iter):
            return False

        return all(a == b for a, b in zip(self.__list, other_iter))


    def __iadd__(self, iterable) :
        
        for obj in iterable:
            self.append(obj)

        return self
        # operator 'o.x += a', translate into o.x = o.x.__iadd__(a)

    def reform(self, other):
        self.clear()
        items = (item.copy() for item in other.__list)
        for item in items:
            self.append(item)


class dobject(metaclass=DObjectMetaClass):

    def __new__(cls, *values, **kwvalues):
        """ 
        ordinal value: in defined order and mro
        """

        instance = super(dobject, cls).__new__(cls)
        attr_values = OrderedDict()
        instance_setattr = super(dobject, instance).__setattr__
        instance_setattr('__value_dict__', attr_values)

        if not values and not kwvalues: # empty object
            return instance

        parameters = OrderedDict(iter_chain(cls.__primary_key__.items(), 
                                            cls.__value_attrs__.items()))

        seen = set()
        # consume parameters with the positional arguments
        for val in values:
            if not parameters:
                raise TypeError('Too much positional argument given')

            attr_name, attr = parameters.popitem(last=False)
            attr_values[attr_name] = cast_attr_value(attr_name, val, attr.type)
            seen.add(attr_name)

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

            if not isinstance(attr, AggregateAttr):
                arg_value = cast_attr_value(arg_name, arg_value, attr.type)
            
            attr_values[arg_name] = arg_value


        # set default values for these left parameters
        for attr_name, attr in parameters.items():
            if isinstance(attr, AggregateAttr):
                attr_values[attr_name] = attr.agg_type(attr.item_type)
            else:
                if isinstance(attr.default, type):
                    attr_values[attr_name] = attr.default()
                elif isinstance(attr.default, 
                        (str, int, float, Decimal, dt.date, dt.time, 
                         dt.datetime, dt.timedelta)):
                    attr_values[attr_name] = attr.default

        
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
        
        pkey_attrs   = []
        val_attrs = []
        if onlywith:
            # for attr_name, attr in self.__class__.__primary_key__.items():
            #     if attr_name in ignore or attr in ignore:
            #         continue

            #     pkey_attrs.append(attr)

            for attr_name, attr in self.__class__.__value_attrs__.items():
                if attr_name not in onlywith or attr not in onlywith:
                    continue

                if attr_name in ignore or attr in ignore:
                    continue

                val_attrs.append(attr)

        else:
            # for attr_name, attr in self.__class__.__primary_key__.items():
            #     if attr_name in ignore or attr in ignore:
            #         continue

            #     pkey_attrs.append(attr)

            for attr_name, attr in self.__class__.__value_attrs__.items():
                if attr_name in ignore or attr in ignore:
                    continue

                val_attrs.append(attr)

        return val_attrs


    def reform(self, src, onlywith=None, ignore=None, link=None):
        """
        Reform the dobject with src's attribues or fields. 

        A reformation of object means that the object is still the object itself. 
        After reformation, primary key attributes are pristine.

        The link is a dictionary defined with the connections between this 
        attribute to some attribue of the other's.

        Note: the onlywith does not applied on primary key attributes. 
        """


        self_val_attrs = self._attrs_filtered(onlywith, ignore)


        self_values = self.__value_dict__

        NO_VALUE = object()

        def copy_value(attr_name, self_val, other_val):
            if other_val is NO_VALUE:
                return

            if isinstance(self_val, daggregate) or isinstance(self_val, dobject):
                self_val.reform(other_val)
                return

            setattr(self, attr_name, other_val)


        if isinstance(src, dict):


            for attr in self_val_attrs:
                attr_name = attr.name
                self_val  = getattr(self, attr_name)
                other_val = other.get(attr_name, NO_VALUE)

                copy_value(attr_name, self_val, other_val)

        elif isinstance(src, (dobject, NamedDict)): # support domobj and sqlblock


            for attr in self_val_attrs:
                attr_name = attr.name
                self_val  = getattr(self, attr_name)
                other_val = getattr(src,  attr_name, NO_VALUE)

                copy_value(attr_name, self_val, other_val)

        else:
            raise TypeError('Unknown type: ' + src.__class__.__name__)

        return self

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


