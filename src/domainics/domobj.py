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
_logger = logging.getLogger(__name__)

import sys
import inspect
from collections import OrderedDict
from collections import namedtuple

import datetime as dt
from decimal import Decimal

def dident(*fields):
    """ """

    # the primary_key is called class block,
    # pass fields to metaclass '__new__'  via _dobj_pks_pendings
    frame = sys._getframe(1)
    if not hasattr(dident, '_dobj_pks_pendings'):
        pendings = {}
        setattr(dident, '_dobj_pks_pendings', pendings)
    else:
        pendings = getattr(dident, '_dobj_pks_pendings')

    pks = []
    for field in fields:
        if not isinstance(field, datt):
            errmsg = 'the primary key \'%s\' is not datt' % field
            raise TypeError(errmsg)
        
        pks.append(field)
        field.is_identity = True

    f_locals = frame.f_locals
    pendings[f_locals['__module__'] + '.' + f_locals['__qualname__']] = pks


class DObjectMetaClass(type):

    @classmethod
    def __prepare__(metacls, name, bases, **kwargs): 
        return OrderedDict()

    def __new__(metacls, classname, bases, class_dict):


        if hasattr(dident, '_dobj_pks_pendings'):
            pk_pendings = getattr(dident, '_dobj_pks_pendings')
            qclsname = class_dict.get('__module__') + '.' + class_dict.get('__qualname__')
            if qclsname in pk_pendings:
                pkeys = pk_pendings.pop(qclsname)
            else:
                pkeys = []
        else:
            pkeys = []


        fields = OrderedDict()
        # set all names of domain field 
        for name in class_dict:
            attr = class_dict[name]
            if isinstance(attr, datt):
                attr.name    = name
                fields[name] = attr
            elif isinstance(attr, dset):
                # In class-block, dset is used, replace it with AggregateAttr
                attr = AggregateAttr(dset, attr.item_type, attr.__doc__)
                attr.name        = name
                class_dict[name] = attr
                fields[name]     = attr


        cls = type.__new__(metacls, classname, bases, class_dict)
        cls._dobj_fields = fields
        
        id_fields = [p.name for p in pkeys]
        cls._dobj_id_names = id_fields

        # assemble the _dobj_id property
        if id_fields:
            id_type   = namedtuple('ID', id_fields)

        def _get_dobj_id(self):
            """return the identity object of domain object; None if no identity is defined"""

            if self.__class__._dobj_id_names:
                values = []
                for attrname in id_fields:
                    val = getattr(self, attrname)
                    if val is None:
                        errmsg = "The id-attribute '%s' cannot be none" % attrname
                        raise TypeError(errmsg) 
                    values.append(val)
                return id_type(*values)
            
            return None

        cls._dobj_id = property(_get_dobj_id)


        return cls


    @property
    def _dobj_attrs(cls):
        if hasattr(cls, '__dobj_attr_dict'):
            return getattr(cls, '__dobj_attr_dict')
        
        mro_attrs = OrderedDict()
        for c in cls.__mro__:
            if c == dobject:
                break

            for name, attr in c._dobj_fields.items():
                if name in mro_attrs:
                    continue

                mro_attrs[name] = attr

        setattr(cls, '__dobj_attr_dict', mro_attrs)
        return mro_attrs


class datt:
    """ """

    __slots__ = ('name', 'datatype', 'default_expr', 'default', 'doc', 'is_identity')

    def __init__(self, type=object, expr=None, default=None, doc=None):
        self.datatype     = type
        self.default_expr = expr
        self.default      = default
        self.doc          = doc
        self.is_identity  = False


    def __get__(self, instance, owner):
        if instance is None: # get domain field
            return self

        attrs = getattr(instance, '_dobject__attrs')
        return attrs.get(self.name)

    def __set__(self, instance, value):
        name = self.name
        if self.is_identity:
            errmsg = "The identity attribute '%s' is read-only" % self.name
            raise TypeError(errmsg)

        attrs  = getattr(instance, '_dobject__attrs')
        now_val = attrs.get(name, None)
        value   = cast_attr_value(name, value, self.datatype)  

        if now_val != value :
            attrs[name] = value

            # changed = getattr(instance, '_dobject__orig')
            # old_val = changed.get(name, None)
            # if old_val == value : # the value is recoveried
            #     del changed[name]
            # else:
            #     if name not in changed:
            #         changed[name] = now_val

    def __delete__(self, instance):
        raise NotImplemented('unsupported field deleting')


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

        val = getattr(instance, '_dobject__attrs').get(self.name)

        return val

        # return getattr(instance, '_dobject__attrs').get(self.name)

    def __set__(self, instance, value):
        oldval = getattr(instance, '_dobject__attrs').get(self.name)

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

    def __init__(self, item_type, iterable=None, doc=None):
        self.__list = []
        self.__map  = {}

        if not isinstance(item_type, type):
            raise TypeError('item_type should be a type object')

        self.item_type   = item_type

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

        obj_id = obj._dobj_id
        if not obj_id:
            errmsg = "The identity(%s) of %s is required" 
            errmsg %= (','.join(self.__class__._dobj_id_names, 
                       self.__class__.__name__))
            raise TypeError(errmsg)
        
        if obj_id in self.__map:
            index = self.__map[obj_id]
            self.__list[index] = obj
        else:
            index = len(self.__list)
            self.__map[obj_id] = index
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

class dobject(metaclass=DObjectMetaClass):

    def __new__(cls, *values, **kwvalues):
        """ 
        ordinal value: in defined order and mro
        """

        instance = super(dobject, cls).__new__(cls)
        attr_values = OrderedDict()
        instance_setattr = super(dobject, instance).__setattr__
        instance_setattr('_dobject__attrs', attr_values)
        instance_setattr('_dobject__orig',  {}) # original values

        if not values and not kwvalues:
            return instance

        fields = []
        for n, f in cls._dobj_attrs.items():
            fields.append((n, f))  

        for val in values:
            name, field = fields.pop(0)
            attr_values[name] = cast_attr_value(name, val, field.datatype)


        for name, attr in fields:
            if isinstance(attr, AggregateAttr):
                val = kwvalues.pop(name, None)
                if val is None:
                    val = attr.agg_type(attr.item_type)
            else:
                val = kwvalues.pop(name, None)
                val = cast_attr_value(name, val, attr.datatype)
                if val is None: # default value
                    if isinstance(attr.default, type):
                        val = attr.default()
                    elif isinstance(attr.default, 
                            (str, int, float, Decimal, dt.date, dt.time, 
                             dt.datetime, dt.timedelta)):
                        val = attr.default

            attr_values[name] = val

        if kwvalues:
            errmsg = 'not defined field: ' 
            errmsg += ', '.join([f for f in kwvalues])
            raise ValueError(errmsg)

        instance._dobj_id

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
        segs = ['%s=%r' % (a, self.__attrs.get(a)) for a in self.__attrs]
        # if self.__orig:
            # args = ', '.join(['%s=%r' % p for p in self.__orig.items() ])
            # segs.append('__orig=(' + args + ')')

        return self.__class__.__name__ + '(' + ', '.join(segs) + ')'

    def __eq__(self, other) :
        ''' ducking equals: 
        1. True if the identity of this object equals that of other;
        2. True if all of fields of this object equal those of other '''

        if other is None :
            return False

        this_id = self._dobj_id
        if this_id is not None and this_id == other._dobj_id:
            return True

        if set(self.__attrs.keys()) != set(other.__attrs.keys()):
            return False

        for name, val in self.__attrs.items():
            if val != other.__attrs[name]:
                return False
        
        return True

    def copy(self):
        self_attrs = getattr(self,   '_dobject__attrs')
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

        self_attrs = getattr(self,   '_dobject__attrs')
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



def cast_attr_value(attrname, val, datatype):
    if val is None:
        return val

    if isinstance(val, datatype):
        return val

    try:
        return datatype(val)
    except (ValueError, TypeError) as ex:
        err = "The attribute '%s' should be \'%s\' type, not '%s'"
        err %= (attrname, datatype.__name__, type(val).__name__)
        raise TypeError(err).with_traceback(ex.__traceback__)           


