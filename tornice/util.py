# -*- coding: utf-8 -*-

import sys as _sys
from keyword import iskeyword as _iskeyword
from collections import OrderedDict as _OrderedDict


_nameddict_class_tmpl = """
class {typename}:
    __fields__ = {field_names!r}

    def __init__(self, {arg_list}):
        self.__dict__.update((k, v) for k, v in zip(self.__fields__, [{arg_list}]))     

    def __repr__(self):
        expr = ','.join(['%s=%r' % (k, getattr(self, k)) for k in self.__fields__])
        return self.__class__.__name__ + '(' + expr + ')'


    def _asdict(self):
        'Return a new OrderedDict which maps field names to their values.'
        return self.__dict__

"""

    # @property
    # def __dict__(self):
    #     print(111)
    #     'A new OrderedDict mapping field names to their values'
    #     return _OrderedDict(zip(self.__fields__, self))
def nameddict(typename, field_names):
    """ """
    
    if isinstance(field_names, str):
        field_names = field_names.replace(',', ' ').split()
    field_names = list(map(str, field_names))


    for name in [typename] + field_names:
        if not name.isidentifier():
            raise ValueError('Type names and field names must be valid '
                             'identifiers: %r' % name)
        if _iskeyword(name):
            raise ValueError('Type names and field names cannot be a '
                             'keyword: %r' % name)

    seen = set()
    for name in field_names:
        if name.startswith('_') and not rename:
            raise ValueError('Field names cannot start with an underscore: '
                             '%r' % name)
        if name in seen:
            raise ValueError('Encountered duplicate field name: %r' % name)
        seen.add(name)

    # Fill-in the class template
    class_definition = _nameddict_class_tmpl.format(
        typename    = typename,
        field_names = tuple(field_names),
        arg_list    = ','.join(field_names),
    )

    namespace = dict(__name__='nameddict_%s' % typename, _OrderedDict=_OrderedDict)
    exec(class_definition, namespace)
    result = namespace[typename]

    try:
        result.__module__ = _sys._getframe(1).f_globals.get('__name__', '__main__')
    except (AttributeError, ValueError):
        pass

    return result

