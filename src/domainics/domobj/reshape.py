# -*- coding: utf-8 -*-

from collections import OrderedDict
from collections.abc import Iterable, Mapping
from decimal import Decimal
from itertools import chain as iter_chain
import sys
from ..util import NamedDict
from .typing import DAttribute


class ReshapeDescriptor:

    def __get__(self, instance, owner):
        if instance is None:
            def op_func(*args, **kwargs):
                return _reshape_class(owner, *args, **kwargs)
            return op_func
        else:
            def op_func(*args, **kwargs):
                return _reshape_object(instance, *args, **kwargs)
            return op_func

def _reshape_object(instance, *args, **kwargs):
    """ """

    if args:
        raise ValueError("The positional argument is not allowable here")

    dobj_cls = instance.__class__
    return dobj_cls(**kwargs)

def _reshape_class(orig_cls, *args, **kwargs):

    new_type_name = None # string of type name
    selected = set() # names
    ignored = set() # names
    new_pkeys = [] # list of names
    declared  = OrderedDict() # {attr_name: attribute}
    new_bases = [] # list of type
    combined  = [] # list of type

    substituted = {} #  {new_attr : old_attr}

    arg_name = '_ignore'
    if arg_name in kwargs:
        arg_value = kwargs[arg_name]
        if isinstance(arg_value, Iterable):
            for i, elem in enumerate(arg_value):
                if isinstance(elem, str):
                    ignored.add(elem)
                elif isinstance(elem, DAttribute):
                    ignored.add(elem.name)
                else:
                    errmsg = ("The %d-th element in 'ignore' argument "
                              "should be a str or DAttribute object: %r")
                    errmsg %= (elem, arg_value)
                    raise ValueError(errmsg)
        elif isinstance(arg_value, DAttribute):
            ignored.add(arg_value.name)
        elif isinstance(arg_value, str):
            ignored.add(arg_value)

        del kwargs[arg_name]

    arg_name = '_key'
    if arg_name in kwargs:
        arg_value = kwargs[arg_name]
        if isinstance(arg_value, Iterable):
            for i, elem in enumerate(arg_value):
                if isinstance(elem, str):
                    new_pkeys.append(elem)
                elif isinstance(elem, DAttribute):
                    new_pkeys.append(elem.name)
                else:
                    errmsg = ("The %d-th element in '_pkeys' argument "
                              "should be a str or DAttribute object: %r")
                    errmsg %= (elem, arg_value)
                    raise ValueError(errmsg)
        elif isinstance(arg_value, DAttribute):
            new_pkeys.append(arg_value.name)
        elif isinstance(arg_value, str):
            new_pkeys.append(arg_value)

        del kwargs[arg_name]

    arg_name = '_base'
    if arg_name in kwargs:
        arg_value = kwargs[arg_name]

        if isinstance(arg_value, type):
            new_bases.append(arg_value)

        elif isinstance(arg_value, Iterable):
            for i, cls in enumerate(arg_value):
                if isinstance(cls, type):
                    new_bases.append(cls)
                else:
                    errmsg = ("The %d-th element of '_base' should be"
                              " a type object")
                    errmsg %= (i + 1)
                    raise ValueError(errmsg)

        else:
            errmsg = ("The value of '_base' should be"
                      " a iterable object of type or a type object")
            raise ValueError(errmsg)

        del kwargs[arg_name]

    arg_name = '_combine'
    if arg_name in kwargs:
        arg_value = kwargs[arg_name]

        if isinstance(arg_value, type):
            combined.append(arg_value)

        elif isinstance(arg_value, Iterable):
            for i, cls in enumerate(arg_value):
                if isinstance(cls, type):
                    combined.append(cls)
                else:
                    errmsg = ("The %d-th element of '_combine' should be"
                              " a type object")
                    errmsg %= (i + 1)
                    raise ValueError(errmsg)

        else:
            errmsg = ("The value of '_combine' should be"
                      " a iterable object of type or a type object")
            raise ValueError(errmsg)

        del kwargs[arg_name]


    arg_name = '_subst'
    if arg_name in kwargs:
        arg_value = kwargs[arg_name]

        if isinstance(arg_value, Mapping):
            for new_attr, old_attr in arg_value.items():
                if isinstance(old_attr, str):
                    substituted[new_attr] = old_attr
                else:
                    errmsg = ("The target or source attribue names should be "
                              " a str object in _subst")
                    raise ValueError(errmsg)
        else:
            raise ValueError("The _subst should be a dict or Mapping object")

        del kwargs[arg_name]


    arg_name = '_name'
    if arg_name in kwargs:
        arg_value = kwargs[arg_name]

        if isinstance(arg_value, str):
            new_type_name = arg_value
        else:
            raise ValueError("The _name should be a str object")

        del kwargs[arg_name]

    for i, arg in enumerate(args):
        if isinstance(arg, str):
            selected.add(arg)
        elif isinstance(arg, DAttribute):
            selected.add(arg.name)
        else:
            errmsg = ("The %d-th argument must be a str or attribute object"
                      ", not : %r")
            errmsg %= (i + 1, arg)
            raise ValueError(errmsg)

    for attr_name, arg_value in kwargs.items():
        if attr_name.startswith('_'):
            raise ValueError("Unknown operation '%s'" % attr_name)

        elif isinstance(arg_value, bool):
            if arg_value:
                selected.add(arg)
            else:
                ignored.add(arg)

        elif(arg_value, DAttribute):
            declared[attr_name] = arg_value

        else:
            errmsg = "Unknown operand: %s=%r" % (attr_name, arg_value)
            raise ValueError(errmsg)

    # -------------------------------------------------------------------


    attributes = OrderedDict()
    for attr_name, attr in iter_chain(orig_cls.__dobject_key__.items(),
                                      orig_cls.__dobject_att__.items()):

        attr = attr.copy()
        attr.owner_class = None
        if attr_name in substituted:
            attributes[substituted[attr_name]] = attr
        else:
            attributes[attr_name] = attr

    # ONLY substitute the original object's attribute names
    for old_attr_name, new_attr_name  in substituted.items():

        if (old_attr_name not in orig_cls.__dobject_att__ and
                old_attr_name not in orig_cls.__dobject_key__):

            errmsg = "No found the attribute '%s' substituted by '%s' in %s"
            errmsg = (old_attr_name, new_attr_name, orig_cls.__name__)
            raise ValueError(errmsg)

        if old_attr_name in selected:
            selected.add(new_attr_name)
            selected.remove(old_attr_name)

        if old_attr_name in ignored:
            ignored.add(new_attr_name)
            ignored.remove(old_attr_name)

    for cls in combined:
        for attr_name, attr in iter_chain(cls.__dobject_key__.items(),
                                          cls.__dobject_att__.items()):

            if attr_name not in attributes:
                attributes[attr_name] = attr

    for attr_name, attr in declared.items():
        attributes[attr_name] = attr

    if selected:
        attributes = OrderedDict([(k, v) for k, v in attributes.items()
                                    if k in selected and k not in ignored])
    else:
        attributes = OrderedDict([(k, v) for k, v in attributes.items()
                                    if k not in ignored])

    if new_pkeys:
        pkeys = []
        for attr_name in new_pkeys:
            if attr_name in ignored:
                errmsg = ("Conflict! The attribute '%s' has specified as "
                          "primary key, and also as ignored attribute")
                errmsg %= attr_name
                raise ValueError(errmsg)

            if attr_name not in attributes:
                errmsg = ("The attribute '%s' specified as primary key does not"
                          " be declared in origin or base classes")
                errmsg %= attr_name
                raise ValueError(errmsg)

            if attr_name in attributes:
                pkeys.append(attr_name)

        new_pkeys = pkeys
    else:
        if orig_cls.__dobject_key__:
            new_pkeys = []
            for attr_name in orig_cls.__dobject_key__:
                if attr_name in substituted:
                    attr_name = substituted[attr_name]

                if attr_name not in attributes:
                    continue

                new_pkeys.append(attr_name)


    attributes['__dobject_key__'] = new_pkeys
    attributes['__dobject_origin_class__'] = orig_cls

    subst_map = OrderedDict()
    for old_name, new_name in substituted.items():
        subst_map[new_name] = old_name

    attributes['__dobject_mapping__'] = subst_map

    if not new_bases:
        new_bases = orig_cls.__bases__
    else:
        new_bases = tuple(new_bases)

    if not new_type_name :
        new_type_name = orig_cls.__name__

    new_cls = type(new_type_name, new_bases, attributes)

    new_cls.__module__ = sys._getframe(2).f_globals.get('__name__', '__main__')

    setattr(new_cls, '__dobject_origin_class__', tuple([orig_cls] + combined))
    if substituted:
        setattr(new_cls, '__dobject_mapping__', substituted)

    return new_cls

class ReshapeOperator:
    __slot__ = ('source', 'requred', 'ignored')

    def __init__(self, source, operands, kwoperands):
        self.source = source
        self.required = OrderedDict()
        self.ignored = OrderedDict()
        self._base = []
        self._primary_key = None
        self._name = None

        self.parse_operands(operands, kwoperands)


    def reshape_class(self):
        """ """

        tmpl_pkey = None
        tmpl_attrs = OrderedDict()
        for cls in iter_chain([self.source], self._base):
            if tmpl_pkey is None and cls.__dobject_key__:
                # The nearest primary key definition is valid
                tmpl_pkey = cls.__dobject_key__

            for attr_name, attr in iter_chain(cls.__dobject_key__.items(),
                                              cls.__dobject_att__.items()):
                if attr_name not in tmpl_attrs:
                    tmpl_attrs[attr_name] = attr

        prop_dict = OrderedDict()
        if self.required:
            for attr_name, attr in tmpl_attrs.items():
                if attr_name not in self.required:
                    continue

                if attr_name in self.ignored:
                    continue

                prop_dict[attr_name] = attr
        else:
            for attr_name, attr in tmpl_attrs.items():
                if attr_name in self.ignored:
                    continue

                prop_dict[attr_name] = attr

        pkey_attrs = []
        for attr in (self._primary_key if self._primary_key else tmpl_pkey):
            if isinstance(attr, str):
                if attr not in prop_dict:
                    continue
                attr = prop_dict[attr]
            else:
                if attr.name not in prop_dict:
                    continue
            pkey_attrs.append(attr)

        prop_dict['__dobject_key__'] = pkey_attrs

        if not self._base:
            # Oops, workaround, avoid cyclical importing!!!
            from ..db.dtable import dtable
            from ._dobject import dobject

            if issubclass(self.source, dtable):
                base_cls = tuple([dtable])
            else:
                base_cls = tuple([dobject])
            # no inheritance, it's too complicated
        else:
            base_cls = tuple(self._base)

        if not self._name:
            self._name = self.source.__name__ # keey the name


        reshaped_cls = type(self._name, base_cls, prop_dict)
        return reshaped_cls
