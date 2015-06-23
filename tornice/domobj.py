
import logging

_logger = logging.getLogger(__name__)


# -*- coding: utf-8 -*-

from collections import OrderedDict as _Dict

class DObjectMetaClass(type):
    @classmethod
    def __prepare__(metacls, name, bases): 
        return _Dict()

    def __new__(metacls, classname, bases, class_dict):

        fields = _Dict()
        # set all names of domain field 
        for name, attr in class_dict.items():
            if isinstance(attr, dfield):
                attr.name = name
                fields[name] = attr

        cls = type.__new__(metacls, classname, bases, class_dict)
        cls.__fields__ = fields
        return cls

class dfield:
    __slots__ = ('name', 'datatype', 'default_expr', '__doc__')

    def __init__(self, type=object, expr=None, doc=None):
        self.datatype     = type
        self.default_expr = expr
        self.__doc__      = doc

    def __get__(self, instance, owner):
        if instance is None: # get domain field
            return self

        return instance.__fields__.get(self.name)

    def __set__(self, instance, value):
        name = self.name
        if not isinstance(value, self.datatype):
            err = 'field %s should be \'%s\' type: value=%r'
            err %= (name, self.datatype.__name__, value)
            raise TypeError(err)

        fields  = instance.__fields__
        now_val = fields.get(name, None)
        if now_val != value :
            fields[name] = value

            changed = instance.__orig__
            old_val = changed.get(name, None)
            if old_val == value : # the value is recoveried
                del changed[name]
            else:
                if name not in changed:
                    changed[name] = now_val

    def __delete__(self, instance):
        raise NotImplemented('unsupported field deleting')

class dlist(dfield):
    
    def __init__(self, type=object, doc=None):
        self.datatype     = type
        self.default_expr = None
        self.__doc__      = doc

class DobjectList(list):

    # def __delitem__(self, index):
    #     raise IndexError

    # @abstractmethod
    # def insert(self, index, value):
    #     'S.insert(index, value) -- insert value before index'
    #     raise IndexError

    # def append(self, value):
    #     'S.append(value) -- append value to the end of the sequence'
    #     self.insert(len(self), value)

    # def clear(self):
    #     'S.clear() -> None -- remove all items from S'
    #     try:
    #         while True:
    #             self.pop()
    #     except IndexError:
    #         pass

    # def reverse(self):
    #     'S.reverse() -- reverse *IN PLACE*'
    #     n = len(self)
    #     for i in range(n//2):
    #         self[i], self[n-i-1] = self[n-i-1], self[i]

    def extend(self, values):
        'S.extend(iterable) -- extend sequence by appending elements from the iterable'
        for v in values:
            self.append(v)

    def pop(self, index=-1):
        '''S.pop([index]) -> item -- remove and return item at index (default last).
           Raise IndexError if list is empty or index is out of range.
        '''
        v = self[index]
        del self[index]
        return v

    def remove(self, value):
        '''S.remove(value) -- remove first occurrence of value.
           Raise ValueError if the value is not present.
        '''
        del self[self.index(value)]

    def __iadd__(self, values):
        self.extend(values)
        return self


# 'append',
#  'clear',
#  'copy',
#  'count',
#  'extend',
#  'index',
#  'insert',
#  'pop',
#  'remove',
#  'reverse',
#  'sort']  

class dobject(metaclass=DObjectMetaClass):

    def __new__(cls, **kwargs):
        """ """

        classes = []
        for c in cls.__mro__:
            if c == dobject: break
            classes.append(c)

        fields = _Dict()
        for c in reversed(classes):
            for name in c.__fields__:
                val = kwargs.pop(name, None)
                if val is None: # default value
                    expr = c.__fields__[name].default_expr
                    val = eval(expr) if expr is not None else None
                
                fields[name] = val

        if kwargs:
            errmsg = 'not defined field: ' 
            errmsg += ', '.join([f for f in kwargs])
            raise ValueError(errmsg)

        instance = super(dobject, cls).__new__(cls)
        instance.__fields__  = fields
        instance.__orig__ = {}

        return instance

    def __getattr__(self, name):
        errmsg ='The domain object %s has no field: %s ' 
        errmsg %= (self.__class__.__name__, name)
        raise AttributeError(errmsg)

    def __repr__(self):
        """ """
        segs = ['%s=%r' % p for p in self.__fields__.items()]
        if self.__orig__:
            args = ', '.join(['%s=%r' % p for p in self.__orig__.items() ])
            segs.append('__orig__=(' + args + ')')

        return self.__class__.__name__ + '(' + ', '.join(segs) + ')'

    def __eq__(self, other) :
        ''' ducking equals: True if all of fields of this object are equaled those of other '''

        if set(self.__fields__.keys()) != other.__fields__.keys():
            return False

        for name, val in self.__fields__.items():
            if val != other.__fields__[name]:
                return False
        
        return True

    def _pristine(self):
        "True if this object are not changed"
        
        if self.__orig__:
            return False

        for field in self.__class__.__mro_fields__:
            if isinstance(field.datatype, dobject):
                if not self.__fields__[field.name]._pristine:
                    return False

        return True

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
