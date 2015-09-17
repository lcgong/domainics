# -*- coding: utf-8 -*-

from collections import OrderedDict
from collections import namedtuple

from collections.abc import Iterable


from .metaclass import datt, daggregate
from .dobject import dobject


class dset(daggregate):
    """
    The aggregate object set of domain object.
    """

    item_type = None

    def __init__(self, item_type, iterable=None, doc=None, primary_key=None):
        self.__list = []
        self.__map = {}

        if not isinstance(item_type, type):
            raise TypeError('item_type should be a type object')

        self.item_type = item_type

        if primary_key is not None:
            if isinstance(primary_key, datt):
                primary_key = OrderedDict([(primary_key.name, primary_key)])

            elif isinstance(primary_key, str):
                if primary_key in item_type.__primary_key__:
                    primary_key = [(primary_key,
                                    item_type.__primary_key__[primary_key])]
                    primary_key = OrderedDict(primary_key)

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

            print(333, primary_key)
            self._item_primary_key = primary_key
            self._item_primary_key_class = namedtuple('PK', (n for n in primary_key))

        else:
            # primary_key is not specified by inialization of dset
            if not item_type.__primary_key__:
                errmsg = "primary key should be given in primary_key argument "
                errmsg += " or be defined in %s class "
                errmsg %= item_type.__name__
                raise ValueError(errmsg)

            self._item_primary_key = item_type.__primary_key__
            self._item_primary_key_class = item_type.__primary_key_class__



        self.__attr_doc  = doc

        if iterable is not None:
            if hasattr(iterable, '__dset__'):
                dset_iter = getattr(iterable, '__dset__')
                for obj in dset_iter(item_type):
                    self.append(obj)
            else:
                for obj in iterable:
                    self.append(obj)


    def add(self, obj):
        """
        If the identity of obj has been added, replace the old one with it.
        """
        if not isinstance(obj, self.item_type):
            errmsg = "The aggregate object should be '%s' type"
            errmsg %= self.item_type.__name__
            raise TypeError(errmsg)

        pkey = self._item_primary_key_class(*tuple(getattr(obj, n)
                                        for n in self._item_primary_key.keys()))
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

        s = '%s(%r, item_type=%s, primary_key=%r)'
        s %= (self.__class__.__name__,
                [obj for obj in self.__list],
                self.item_type.__module__ + '.' + self.item_type.__qualname__,
                tuple(self._item_primary_key.keys()))

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
