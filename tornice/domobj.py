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


def identity(*fields):
    """ """

    # the primary_key is called class block,
    # pass fields to metaclass '__new__'  via _dobj_pks_pendings
    frame = sys._getframe(1)
    if not hasattr(identity, '_dobj_pks_pendings'):
        pendings = {}
        setattr(identity, '_dobj_pks_pendings', pendings)
    else:
        pendings = getattr(identity, '_dobj_pks_pendings')

    pks = []
    for field in fields:
        if not isinstance(field, dattr):
            errmsg = 'the primary key \'%s\' is not dattr' % field
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


        if hasattr(identity, '_dobj_pks_pendings'):
            pk_pendings = getattr(identity, '_dobj_pks_pendings')
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
            if isinstance(attr, dattr):
                attr.name    = name
                fields[name] = attr
            elif isinstance(attr, aggset):
                # In class-block, aggset is used, replace it with AggregateAttr
                attr = AggregateAttr(aggset, attr.item_type, attr.__doc__)
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

class dattr:
    """ """

    __slots__ = ('name', 'datatype', 'default_expr', 'doc', 'is_identity')

    def __init__(self, type=object, expr=None, doc=None):
        self.datatype     = type
        self.default_expr = expr
        self.is_identity  = False
        self.doc          = doc


    def __get__(self, instance, owner):
        if instance is None: # get domain field
            return self

        val = getattr(instance, '_dobject__attrs').get(self.name)
        return val

    def __set__(self, instance, value):
        name = self.name
        if self.is_identity:
            errmsg = "The identity attribute '%s' is read-only" % self.name
            raise TypeError(errmsg)

        value = cast_attr_value(name, value, self.datatype)
  

        attrs  = getattr(instance, '_dobject__attrs')
        now_val = attrs.get(name, None)

        if now_val != value :
            attrs[name] = value

            changed = getattr(instance, '_dobject__orig')
            old_val = changed.get(name, None)
            if old_val == value : # the value is recoveried
                del changed[name]
            else:
                if name not in changed:
                    changed[name] = now_val

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
            errmsg %= (self.name, self.agg_type.__name__, value.__class__.__name__)
            raise TypeError(errmsg)
        
        
        oldval.clear()
        for r in value:
            oldval.append(r)

class daggregate:
    pass

class aggset(daggregate):
    """
    The aggregate object set of domain object.
    """

    def __init__(self, item_type, iterable=None, doc=None):
        self.__list = []
        self.__map  = {}

        if not isinstance(item_type, type):
            raise TypeError('item_type should be a type object')

        self.item_type   = item_type

        self.__attr_doc = doc

        if iterable is not None:
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
            errmsg %= (','.join(self.__class__._dobj_id_names, self.__class__.__name__))
            raise TypeError(errmsg)
        
        if obj_id in self.__map:
            index = self.__map[obj_id]
            self.__list[index] = obj
        else:
            index = len(self.__list)
            self.__map[obj_id] = index
            self.__list.append(obj)


    def remove(self, obj):
        index = self.__map.get(obj._dobj_id)
        if index is None:
            raise ValueError("%r not in aggset" % obj)

        del self.__list[index]

    def clear(self):
        self.__list.clear()
        self.__map.clear()

    def index(self, obj):
        dobj_id = obj._dobj_id
        index = self.__map.get(dobj_id)
        if index is None:
            raise ValueError ('no value of the identity %r' % dobj_id)
        return index

    def __bool__(self):
        return bool(self.__list)

    def __len__(self):
        return len(self.__list)

    def __iter__(self):
        for itemobj in self.__list:
            yield itemobj

    def __repr__(self):
        s = 'aggset('
        s += ', '.join([repr(obj) for obj in self.__list])
        s += ')'
        return s


    def __getitem__(self, index):
        if isinstance(index, slice):
            return aggset(self.item_type, self.__list.__getitem__(index))
        elif isinstance(index, int):
            return self.__list[index]
        elif isinstance(index, dobject):
            dobj_id = index._dobj_id
            index = self.__map.get(dobj_id)
            if index is None:
                raise KeyError('no identity : %r' % dobj_id)

            return self.__list[index]


        errmsg = 'unknown index or slice %s(%r)'
        errmsg %= (index.__class__.__name__, index)
        raise TypeError(errmsg)


    def __eq__(self, other):
        if isinstance(other, aggset):
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


class DomainObject(metaclass=DObjectMetaClass):
    
    @property
    def _pristine(self):
        raise NotImplementedError()

    def _set_pristine(self):
        raise NotImplementedError()
    



def cast_attr_value(attrname, val, datatype):
    if val is None:
        return val

    if isinstance(val, datatype):
        return val

    try:
        return datatype(val)
    except ValueError as ex:
        err = "The attribute '%s' should be \'%s\' type: value=%r(%s)"
        err %= (attrname, datatype, val, type(val).__name__)
        raise TypeError(err)            

class dobject(DomainObject):

    def __new__(cls, *values, **kwvalues):
        """ 
        ordinal value: in defined order and mro
        """

        seen = set()
        fields = []
        for c in cls.__mro__:
            if c == dobject: 
                break

            for n, f in c._dobj_fields.items():
                seen.add(n)
                fields.append((n, f))

        attr_values = OrderedDict()
        for val in values:
            name, field = fields.pop(0)
            attr_values[name] = cast_attr_value(name, val, field.datatype)


        for name, attr in fields:
            if isinstance(attr, AggregateAttr):
                val = attr.agg_type(attr.item_type)
            else:
                val = kwvalues.pop(name, None)
                val = cast_attr_value(name, val, attr.datatype)
                if val is None: # default value
                    expr = attr.default_expr
                    if expr is not None:
                        val = eval(expr)
                    else:
                        val = None

            attr_values[name] = val

        if kwvalues:
            errmsg = 'not defined field: ' 
            errmsg += ', '.join([f for f in kwvalues])
            raise ValueError(errmsg)

        instance = super(dobject, cls).__new__(cls)
        instance.__attrs = attr_values
        instance.__orig  = {} # original atribute values

        instance._dobj_id

        return instance

    def __getattr__(self, name):
        errmsg ='The domain object %s has no field: %s ' 
        errmsg %= (self.__class__.__name__, name)
        raise AttributeError(errmsg)



    def __repr__(self):
        """ """
        segs = ['%s=%r' % (a, self.__attrs.get(a)) for a in self.__attrs]
        if self.__orig:
            args = ', '.join(['%s=%r' % p for p in self.__orig.items() ])
            segs.append('__orig=(' + args + ')')

        return self.__class__.__name__ + '(' + ', '.join(segs) + ')'

    def __eq__(self, other) :
        ''' ducking equals: 
        1. True if the identity of this object equals that of other;
        2. True if all of fields of this object equal those of other '''

        this_id = self._dobj_id
        if this_id is not None and this_id == other._dobj_id:
            return True


        if set(self.__attrs.keys()) != set(other.__attrs.keys()):
            return False



        for name, val in self.__attrs.items():
            if val != other.__attrs[name]:
                return False
        
        return True

    def _pristine(self):
        "True if this object are not changed"
        
        if self.__dobj_orig:
            return False

        for field in self.__class__.__mro_fields__:
            if isinstance(field.datatype, dobject):
                if not self.__fields__[field.name]._pristine:
                    return False

        return True

    @classmethod
    def primary_keys(cls, *fields):
        for field in fields:
            if not isinstance(field, dattr):
                errmsg = 'the primary key \'%s\' is not dattr' % field
                raise TypeError(errmsg)

            cls._primary_keys.add(field)

    def get_primary_keys(cls):
        return

    @classmethod
    def __get_field__(cls, name):
        
        field = cls.__fields__.get(name, None)
        if field: return field

        for c in cls.__mro__:
            if cls == dobject: break

            field = c.__fields__.get(name, None)
            if field: return field

        return None

    @classmethod
    def __mro_fields__(cls):
        
        seen = set()
        for field in cls.__fields__:
            seen.add(field)
            yield field

        for c in cls.__mro__:
            if cls == dobject:
                break

            for field in c.__fields__:
                if field in seen:
                    continue

                seen.add(field)
                yield field




class agglist(DomainObject):
    """
    Aggregate
    """

    def __init__(self, item_type, iterable):
        self.identified   = OrderedDict()
        self.unidentified = []
        self.item_type    = item_type

        for obj in iterable:
            self._dobj_append(obj)
    
    def _dobj_append(self, obj):

        if not isinstance(obj, self.item_type):
            errmsg = "The aggregate object should be '%s' type" 
            errmsg %= self.item_type.__name__
            raise TypeError(errmsg)

        obj_id = obj._dobj_id
        if obj_id:
            errmsg = "missing identity"
            raise TypeError()
        self.identified[obj_id] = obj

    def append(self, obj):
        self._dobj_append(obj)


    # @property
    # def _pristine(self):
    #     "True if this object are not changed"

    #     if self != self.__orig_list__:
    #         return False

    #     for value in self:
    #         if isinstance(value, DomainObject):
    #             if not value._pristine:
    #                 return False

    #     return True

    # def _set_pristine(self):
    #     self.__orig_list__.clear()
    #     self.__orig_list__.extend(self)

    #     for value in self:
    #         if isinstance(value, DomainObject):
    #             value._set_pristine()

    # def _dobj_fire_changed(self):
    #     print('changed')

    # def __delitem__(self, i):
    #     self._dobj_fire_changed()
    #     return  super(dlist, self).__delitem__(i)

    # def __iadd__(self, l):
    #     self._dobj_fire_changed()
    #     return  super(dlist, self).__iadd__(l)

    # def __imul__(self, n):
    #     self._dobj_fire_changed()
    #     return  super(dlist, self).__imul__(n)   

    # def __setitem__(self, i, x):
    #     self._dobj_fire_changed()
    #     return  super(dlist, self).__setitem__(i, x)


    # def insert(self, idx, obj):
    #     self._dobj_fire_changed()
    #     return  super(dlist, self).insert(idx, obj)

    # def clear(self):
    #     self._dobj_fire_changed()
    #     return  super(dlist, self).clear()

    # def extend(self, iterable):
    #     self._dobj_fire_changed()
    #     return  super(dlist, self).extend(iterable)

    # def pop(self, *args):
    #     self._dobj_fire_changed()
    #     return  super(dlist, self).pop(*args)

    # def remove(self, value):
    #     self._dobj_fire_changed()
    #     return  super(dlist, self).remove(value)

    # def reverse(self):
    #     self._dobj_fire_changed()
    #     return  super(dlist, self).reverse()

    # def sort(self, key=None, reverse=False):
    #     self._dobj_fire_changed()
    #     return  super(dlist, self).reverse(key, reverse)



# class DCollectionObject(object):
#     _dobj_

