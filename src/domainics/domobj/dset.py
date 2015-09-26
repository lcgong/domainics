# -*- coding: utf-8 -*-

from collections import OrderedDict
from collections import namedtuple
from collections.abc import Iterable, Mapping

# from .metaclass import datt
# from ._reshape import reshape

from .typing import DObject, DSet, DAttribute
from .metaclass import _make_pkey_class


class dset(DSet):
    """The set of dobjects.
    """

    item_type = None

    def __init__(self, item_type, iterable=None, doc=None, item_key=None):
        self.__list = []
        self.__map = {}

        if not isinstance(item_type, type):
            raise TypeError('item_type should be a type object')

        self.item_type = item_type

        if item_key is not None:
            if isinstance(item_key, DAttribute):
                item_key = OrderedDict([(item_key.name, item_key)])

            elif isinstance(item_key, str):
                if item_key in item_type.__primary_key__:
                    item_key = [(item_key,
                                    item_type.__primary_key__[item_key])]
                    item_key = OrderedDict(primary_key)

                elif item_key in item_type.__value_attrs__:
                    item_key = OrderedDict([(item_key,
                                        item_type.__value_attrs__[item_key])])
                else:
                    errmsg = "No '%s' attribute is defined in %s"
                    errmsg %= (item_key, item_type.__name__)
                    raise ValueError(errmsg)

            elif isinstance(item_key, Iterable):
                pkeys = OrderedDict()
                for attr in item_key:
                    if isinstance(attr, DAttribute):
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
                        raise ValueError('item_key should be a DAttribute, '
                                         'str object or a collection of it')
                item_key = pkeys

            else:
                raise TypeError('item_key should be a DAttribute object '
                                'or an iterable of DAttribute object')

            self.__item_key__ = item_key
            self.__item_key_class__ =  _make_pkey_class(item_type,
                                                        attr_names=item_key)

        else:
            # item_key is not specified by inialization of dset
            if not item_type.__primary_key__:
                errmsg = "primary key should be given in item_key argument "
                errmsg += " or be defined in %s class "
                errmsg %= item_type.__name__
                raise ValueError(errmsg)

            self.__item_key__ = item_type.__primary_key__
            self.__item_key_class__ = item_type.__primary_key_class__



        self.__attr_doc  = doc
        #
        # if iterable is None and from is not None:
        #     iterable = from

        if iterable is not None:
            if hasattr(iterable, '__dset__'):
                dset_iter = getattr(iterable, '__dset__')
                for obj in dset_iter(self.item_type):
                    self.add(obj)
            else:
                for obj in iterable:
                    self.add(obj)


    def add(self, obj):
        """
        If the identity of obj has been added, replace the old one with it.
        """
        if isinstance(obj, DObject):
            if obj.__class__ != self.item_type:
                # reshape the object because of the different doject clasess
                obj = self.item_type(reshape(obj))
        elif isinstance(obj, Mapping):
            obj = self.item_type(reshape(obj)) # reshape the dict object
        else:
            errmsg = ("The aggregate object should be "
                      "DObject or mapping object: %r")
            errmsg %= obj
            raise TypeError(errmsg)

        pkey = self.__item_key_class__(obj)
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

        if isinstance(obj, int):
            raise ValueError('TBD:')
        elif isinstance(obj, tuple):
            pkey_obj = self.__item_key_class__(obj)
        elif isinstance(obj, dict):
            pkey_obj = self.__item_key_class__(**obj)
        elif isinstance(obj, DObject):
            if (self.__item_key_class__ !=
                    obj.__class__.__primary_key_class__):
                pkey_obj = self.__item_key_class__(
                    *(getattr(obj, attr_name)
                        for attr_name in self.__item_key__))
            else:
                pkey_obj = obj.__primary_key__

        else:
            errmsg = 'The type of object should be DObject, identity or int: %s'
            errmsg %= obj.__class__.__name__
            raise TypeError(errmsg)

        index = self.__map.get(pkey_obj)
        if index is None:
            errmsg = 'no value found by the primary key:  %r <- %r'
            errmsg %= (pkey_obj, obj)
            raise ValueError (errmsg)

        return index

    def copy(self):
        """get a copy of the aggregate object and copies of its items"""

        items = (item.copy() for item in self.__list)
        return dset(self.item_type, items)

    def export(self):
        """export dset object in list"""

        return [item.export() for item in self.__list]

    def __bool__(self):
        return bool(self.__list)

    def __len__(self):
        return len(self.__list)

    def __iter__(self):
        for itemobj in self.__list:
            yield itemobj

    def __repr__(self):

        s = '%s(%r, item_type=%s, primary_key=%r)'
        s %= (self.__class__.__name__,
                [obj for obj in self.__list],
                self.item_type.__module__ + '.' + self.item_type.__qualname__,
                tuple(self.__item_key__.keys()))

        return s

    def __getitem__(self, index):

        if isinstance(index, DObject) or isinstance(index, tuple):
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

        if isinstance(index, DObject) :
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
            del self.__map[item.__primary_key__]

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
        if not isinstance(value, DObject):
            raise TypeError('The assigned value should be DObject')

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

        elif isinstance(index, DObject):
            self.__list[self.index(index) ] = value
        elif isinstance(index, slice):
            raise NotImplementedError()
        else:
            errmsg = 'unknown index or slice %s(%r)'
            errmsg %= (index.__class__.__name__, index)
            raise TypeError(errmsg)

    def __hash__(self):
        return hash(self)

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
            self.add(obj)

        return self
        # operator 'o.x += a', translate into o.x = o.x.__iadd__(a)
