# -*- coding: utf-8 -*-

from itertools import chain as iter_chain
from collections import OrderedDict

import datetime as dt
from decimal import Decimal

from ..util import NamedDict

from collections.abc import Iterable, Mapping

from .metaclass import DObjectMetaClass, datt, daggregate, AggregateAttr


def reshape(source, *args, **kwargs):
    """Reshape a domain object into a new domain object.

    Attributes required
    reshape(source_object, 'attr_name1', attr2, attr3=True)

    Attributes ignored:
    reshape(source_object, ignored_attr1=False, ignored_attr2=False)
    reshape(source_object, ignore=('ignored_attr1', ignored_attr2))
    """
    definition = ReshapeOperator(source)

    for i, arg in enumerate(args):
        if isinstance(arg, str):
            definition.required[arg] = True
        elif isinstance(arg, datt):
            definition.required[arg.name] = True
        else:
            errmsg = "The %dth argument should be a str or datt object: %r"
            errmsg %= (i + 1, arg)
            raise ValueError(errmsg)

    for arg, arg_value in kwargs.items():
        if arg == 'ignore' and isinstance(arg_value, Iterable):
            for i, elem in enumerate(arg_value):  # ignore=(attr1, 'attr2')
                if isinstance(elem, str):
                    definition.ignored[elem] = True
                elif isinstance(elem, datt):
                    definition.ignored[elem.name] = True
                else:
                    errmsg = ("The %d-th element in 'ignore' argument "
                              "should be a str or datt object: %r")
                    errmsg %= (elem, arg_value)
                    raise ValueError(errmsg)
        elif arg == 'ignore' and isinstance(arg_value, datt):
            definition.ignored[arg_value.name] = True

        elif isinstance(arg_value, bool):
            if arg_value:
                definition.required[arg] = True
            else:
                definition.ignored[arg] = True

        else:
            errmsg = ("The keyword argument(%s) should be "
                      "True or False, not: %r")
            errmsg %= (arg, arg_value)
            raise ValueError(errmsg)

    return definition

class ReshapeOperator:
    __slot__ = ('source', 'requred', 'ignored')

    def __init__(self, source):
        self.source = source
        self.required = OrderedDict()
        self.ignored = OrderedDict()
