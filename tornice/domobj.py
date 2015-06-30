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
        if not isinstance(field, dfield):
            errmsg = 'the primary key \'%s\' is not dfield' % field
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
        for name, attr in class_dict.items():
            if isinstance(attr, dfield):
                attr.name = name
                fields[name] = attr

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

class dfield:
    __slots__ = ('name', 'datatype', 'default_expr', '__doc__', 'is_identity')

    def __init__(self, type=object, expr=None, doc=None):
        self.datatype     = type
        self.default_expr = expr
        self.is_identity  = False
        self.__doc__      = doc


    def __get__(self, instance, owner):
        if instance is None: # get domain field
            return self

        return getattr(instance, '_dobject__attrs').get(self.name)

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


        for name, field in fields:
            val = kwvalues.pop(name, None)
            val = cast_attr_value(name, val, field.datatype)
            if val is None: # default value
                expr = field.default_expr
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
        segs = ['%s=%r' % p for p in self.__attrs.items()]
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


        if set(self.__fields__.keys()) != set(other.__fields__.keys()):
            return False



        for name, val in self.__fields__.items():
            if val != other.__fields__[name]:
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
            if not isinstance(field, dfield):
                errmsg = 'the primary key \'%s\' is not dfield' % field
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



class agg(DomainObject):
    """
    Aggregate
    """

    def __init__(self, item_type, iterable=None):
        self.identified   = OrderedDict()
        self.unidentified = []
        self.item_type = item_type

        if iterable is not None:
            for obj in iterable:
                self._dobj_append(obj)

    
    def _dobj_append(self, obj):
        obj_id = obj._dobj_id
        if obj_id:
            self.identified[obj_id] = obj
        else:
            self.unidentified.append(obj)

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

    def _dobj_fire_changed(self):
        print('changed')

    def __delitem__(self, i):
        self._dobj_fire_changed()
        return  super(dlist, self).__delitem__(i)

    def __iadd__(self, l):
        self._dobj_fire_changed()
        return  super(dlist, self).__iadd__(l)

    def __imul__(self, n):
        self._dobj_fire_changed()
        return  super(dlist, self).__imul__(n)   

    def __setitem__(self, i, x):
        self._dobj_fire_changed()
        return  super(dlist, self).__setitem__(i, x)


    def insert(self, idx, obj):
        self._dobj_fire_changed()
        return  super(dlist, self).insert(idx, obj)

    def clear(self):
        self._dobj_fire_changed()
        return  super(dlist, self).clear()

    def extend(self, iterable):
        self._dobj_fire_changed()
        return  super(dlist, self).extend(iterable)

    def pop(self, *args):
        self._dobj_fire_changed()
        return  super(dlist, self).pop(*args)

    def remove(self, value):
        self._dobj_fire_changed()
        return  super(dlist, self).remove(value)

    def reverse(self):
        self._dobj_fire_changed()
        return  super(dlist, self).reverse()

    def sort(self, key=None, reverse=False):
        self._dobj_fire_changed()
        return  super(dlist, self).reverse(key, reverse)

class dcollection(dfield):
    # grouped by pks, and sort in 
    pass

# class DCollectionObject(object):
#     _dobj_

