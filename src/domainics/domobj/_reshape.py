# -*- coding: utf-8 -*-

from collections import OrderedDict
from collections.abc import Iterable, Mapping
from decimal import Decimal
from itertools import chain as iter_chain

from ..util import NamedDict
from .metaclass import DObjectMetaClass, datt, daggregate, AggregateAttr

def reshape(source, *operands, **kwoperands):
    """Reshape a domain object into a new domain object.

    Two mode:
    reshape(dobject_class, ...)  :
    reshape(dobject_object, ...) :

    Option arguments:
    _ignore :
    _primary_key :
    _base : base classes
    _name : a new classes name

    Attributes required
    reshape(source_object, 'attr_name1', attr2, attr3=True)

    Attributes ignored:
    reshape(source_object, ignored_attr1=False, ignored_attr2=False)
    reshape(source_object, ignore=('ignored_attr1', ignored_attr2))
    """

    operator = ReshapeOperator(source, operands, kwoperands)
    if isinstance(source, type):
        return operator.reshape_class()
    else:
        return operator

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

    def parse_operands(self, operands, kwoperands):

        for i, arg in enumerate(operands):
            if isinstance(arg, str):
                self.required[arg] = True
            elif isinstance(arg, datt):
                self.required[arg.name] = True
            else:
                errmsg = "The %dth argument should be a str or datt object: %r"
                errmsg %= (i + 1, arg)
                raise ValueError(errmsg)

        for arg, arg_value in kwoperands.items():
            if arg == '_ignore':
                if isinstance(arg_value, Iterable):
                    for i, elem in enumerate(arg_value):
                        # _ignore=(attr1, 'attr2')
                        if isinstance(elem, str):
                            self.ignored[elem] = True
                        elif isinstance(elem, datt):
                            self.ignored[elem.name] = elem
                        else:
                            errmsg = ("The %d-th element in 'ignore' argument "
                                      "should be a str or datt object: %r")
                            errmsg %= (elem, arg_value)
                            raise ValueError(errmsg)
                elif isinstance(arg_value, datt):
                    self.ignored[arg_value.name] = attr
                elif isinstance(arg_value, str):
                    self.ignored[arg_value] = True
            elif arg == '_base' :
                if isinstance(arg_value, type):
                    self._base = [arg_value]

                elif isinstance(arg_value, Iterable):
                    self._base = []
                    for cls in arg_value:
                        if isinstance(cls, type):
                            self._base.append(cls)
                        else:
                            raise ValueError()
                else:
                    raise ValueError()
            elif arg == '_name' :
                if isinstance(arg_value, str):
                    self._name = arg_value
                else:
                    raise ValueError()
            elif arg == '_primary_key':
                if isinstance(arg_value, (datt, str)):
                    self._primary_key = [arg_value]

                elif isinstance(arg_value, Iterable):
                    self._primary_key = []
                    for elem in arg_value:
                        if isinstance(elem, (datt, str)):
                            self._primary_key.append(elem)
                        else:
                            raise ValueError()

                else:
                    raise ValueError()

            elif isinstance(arg_value, bool):
                if arg_value:
                    self.required[arg] = True
                else:
                    self.ignored[arg] = True

            else:
                errmsg = ("The keyword argument(%s) should be "
                          "True or False, not: %r")
                errmsg %= (arg, arg_value)
                raise ValueError(errmsg)

    def reshape_class(self):
        """ """

        tmpl_pkey = None
        tmpl_attrs = OrderedDict()
        for cls in iter_chain([self.source], self._base):
            if tmpl_pkey is None and cls.__primary_key__:
                # The nearest primary key definition is valid
                tmpl_pkey = cls.__primary_key__

            for attr_name, attr in iter_chain(cls.__primary_key__.items(),
                                              cls.__value_attrs__.items()):
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

        prop_dict['__primary_key__'] = pkey_attrs

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

    def reshape_object(self, instance):
        """ """
        this_class = instance.__class__

        selected_attrs = OrderedDict()
        if self.required:
            for attr_name, attr in iter_chain(
                                        this_class.__primary_key__,items(),
                                        this_class.__value_attrs__.items()):

                if attr_name not in self.requred:
                    continue

                if attr_name in self.ignored:
                    continue
                selected_attrs[attr_name] = attr

        else:
            for attr_name, attr in iter_chain(
                                        this_class.__primary_key__.items(),
                                        this_class.__value_attrs__.items()):

                if attr_name not in self.ignored:
                    selected_attrs[attr_name] = attr

        from ._dobject import dobject
        if isinstance(self.source, (dobject, NamedDict)):
            for attr_name, attr in selected_attrs.items():
                if hasattr(self.source, attr_name):
                    attr_val = getattr(self.source, attr_name)
                    attr.set_value_unguardedly(instance, attr_val)

        elif isinstance(self.source, Mapping):
            for attr_name, attr in selected_attrs.items():
                if attr_name in self.source:
                    attr.set_value_unguardedly(instance,
                                               self.source[attr_name])
