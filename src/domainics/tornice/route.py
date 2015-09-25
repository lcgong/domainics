# -*- coding: utf-8 -*-

import logging

import re
import inspect
import types
import datetime
import functools
from importlib import import_module
from collections import namedtuple, OrderedDict
from tornado.web import RequestHandler, HTTPError
from decimal import Decimal


from ..pillar import _pillar_history, pillar_class
from ..util   import comma_split, filter_traceback
from ..domobj import dset, dobject
from ..error  import AuthenticationError
from .. import json as _json

from urllib.parse import urljoin

from ..busitier import _busilogic_pillar, BusinessLogicLayer

from .funchandler import BaseFuncRequestHandler, RESTFuncRequestHandler


def route_base(base_path):

    # get routes from the invoking module
    caller_obj = inspect.currentframe().f_back
    route_table = RouteSpecTable.get_table(caller_obj)
    route_table._part_base_path = base_path

    parent_pkgs = []
    pkg_name = route_table.service_module.__name__.rpartition('.')[0]
    while pkg_name:
        parent_pkgs.append(pkg_name)
        pkg_name = pkg_name.rpartition('.')[0]

    parent_path = ''
    for pkg_name in reversed(parent_pkgs):
        pkg_module = import_module(pkg_name)
        if hasattr(pkg_module, '__http_route_spec_table__'):
            part_path = pkg_module.__http_route_spec_table__._part_base_path
            parent_path = urljoin(parent_path, part_path)

    route_table.base_path = urljoin(parent_path, base_path)

class HTTPRouteSpec():

    def __init__(self, proto):
        self.module_name = None   # name of service module
        self.service_name = None  # name of service function
        self.proto = proto        # http protocol
        self.methods = []         # http methods
        self.path = ''            # the path of service endpoint
        self.path_signature = OrderedDict()
        self.path_pattern = None
        self.handler_class = None

    def add_method(self, method):
        self.methods.append(method)

    def __call__(self, path):
        self.path = path

        def decorator(service_func):
            spec_table = RouteSpecTable.get_table(service_func)
            spec_table.set_rule(self, service_func)

            return service_func

        return decorator

    @property
    def GET(self) :
        self.methods.append('GET')
        return self

    @property
    def POST(self) :
        self.methods.append('POST')
        return self

    @property
    def PUT(self) :
        self.methods.append('PUT')
        return self

    @property
    def DELETE(self) :
        self.methods.append('DELETE')
        return self

class RouteSpecTable:

    def __init__(self, service_module):
        self.service_module = service_module
        self.base_path = ''
        self._specs = []

    @staticmethod
    def get_table(service_func):
        """Get http route table in the module that declare the service func"""

        service_module = inspect.getmodule(service_func)
        if hasattr(service_module, '__http_route_spec_table__'):
            return service_module.__http_route_spec_table__

        table = RouteSpecTable(service_module)
        setattr(service_module, '__http_route_spec_table__', table)

        return table


    def set_rule(self, route_spec, service_func):

        if not inspect.isfunction(service_func):
            errmsg = "The service function '%s' in '%s' should be a function"
            errmsg %= (service_func.__name__, service_module.__name__)
            raise TypeError(errmsg)

        route_spec.module_name = self.service_module.__name__
        route_spec.service_name = service_func.__name__

        route_spec.path = urljoin(self.base_path, route_spec.path)

        # parse the argments in service path
        path_pattern, path_args = _parse_route_rule(route_spec.path)
        print(666, route_spec.path, path_pattern)
        route_spec.path_signature = path_args
        route_spec.path_pattern = path_pattern

        self._specs.append(route_spec)

    @property
    def route_specs(self):
        return self._specs

    def setup(self):
        # This process must be after all service function are decorated
        for spec in self._specs:
            spec.handler_class = self._make_handler_class(spec)

    def _make_handler_class(self, route_spec):

        try:
            # make sure that decorated-order is irrevalent!!!
            # There are decorators will decorator this function laterly.
            # To make these decorators available, refresh these service function
            service_func = getattr(self.service_module, route_spec.service_name)
        except AttributeError as ex:
            errmsg = "Could not find '%s' service function in module '%s'. "
            errmsg %= (service_func.__name__, self.service_module.__name__)
            errmsg += "The function or its name shoule be declared in module."
            raise AttributeError(errmsg)

        class_dict = dict(
            # service_func is function that has no 'self' parameter
            __module__      = route_spec.module_name,
            service_name    = route_spec.service_name,
            service_func    = staticmethod(service_func),
            path_signature  = route_spec.path_signature
            )

        if route_spec.proto == 'REST':
            base_classes = (RESTFuncRequestHandler,)
        elif route_spec.proto == 'HTTP':
            base_classes = (BaseFuncRequestHandler,)

        class_name = route_spec.service_name + '_'
        class_name += route_spec.proto.lower() + '_handler'

        cls = type(class_name, base_classes, class_dict)
        for m in route_spec.methods:
            setattr(cls, m.lower(), cls.do_handler_func)

        return cls



class HTTPRouteSpecDecoratorFactory():

    def __init__(self, proto):
        self.proto = proto

    def __call__(self, path, methods=None):
        print(9999, path)
        route_spec = HTTPRouteSpec(self.proto)
        if methods:
            for method in comma_split(methods):
                route_spec.add_method(method)

        route_spec.path = path
        return route_spec

    @property
    def GET(self) :
        return HTTPRouteSpec(self.proto).GET

    @property
    def POST(self) :
        return HTTPRouteSpec(self.proto).POST

    @property
    def PUT(self) :
        return HTTPRouteSpec(self.proto).PUT

    @property
    def DELETE(self) :
        return HTTPRouteSpec(self.proto).DELETE


rest = HTTPRouteSpecDecoratorFactory('REST')
http = HTTPRouteSpecDecoratorFactory('HTTP')



def _cast_arg_list(arg, filter=None):
    """cast element list arg into tuple arg

    :param arg: 'GET POST', 'GET, POST' or  ['GET', 'POST']

    filter each element with a argument function
    """

    if isinstance(arg, tuple) or isinstance(arg, list):
        if filter is not None:
            return tuple(filter(a) for a in arg)
        else:
            return tuple(arg)

    args = (s.strip() for s in  arg.replace(',', ' ').split())
    if filter is not None:
        return tuple(filter(s) for s in args)

    return tuple(args)

#
# def _get_route_table(obj):
#     handler_module = inspect.getmodule(obj)
#     if not hasattr(handler_module, '_module_route_table'):
#         route_table = ModuleRouteTable(handler_module)
#         setattr(handler_module, '_module_route_table', route_table)
#     else:
#         route_table = handler_module._module_route_table
#
#     return route_table


_route_rule_syntax = re.compile('(\\\\*)'
    '(?:(?::([a-zA-Z_][a-zA-Z_0-9]*)?)'
    '|({([a-zA-Z_][a-zA-Z_0-9]*)?(?::(str|int|float|path|'
    '((?:\\\\.|[^\\\\}]+)+)?)?|([^:]+))?}))')


# from bottle project
def _re_flatten(p):
    """ Turn all capturing groups in a regular expression pattern into
        non-capturing groups. """
    if '(' not in p:
        return p

    def repl(m):
        # print(m.groups(), len(m.group(1)))
        if len(m.group(1)) % 2:
            return m.group(0)
        else:
            return m.group(1) + '(?:'

    return re.sub(r'(\\*)(\(\?P<[^>]+>|\((?!\?))', repl, p)


_route_rule_filters = {
    'str'  : lambda conf: (_re_flatten(conf or '[^/]+'), None, str),
    'int'  : lambda conf: (r'-?\d+', int, int),
    'float': lambda conf: (r'-?[\d.]+', float, float),
    'path' : lambda conf: (r'.+?', None, str)
}


def _parse_tokens(rule):
    offset, prefix = 0, ''
    for match in _route_rule_syntax.finditer(rule):
        prefix += rule[offset:match.start()]
        g = match.groups()
        if len(g[0]) % 2:  # Escaped wildcard
            prefix += match.group(0)[len(g[0]):]
            offset = match.end()
            continue

        if prefix:
            yield prefix, None, None, (match.start(), match.end())


        if g[1] is None :
            if g[3] is None:
                name, filtr, conf = None, 'str', g[6]
            else:
                name, filtr, conf = g[3:6]
        else:
            name, filtr, conf = g[1], 'str', None

        yield name, filtr, conf or None, (match.start(), match.end())
        offset, prefix = match.end(), ''

    if offset <= len(rule) or prefix:
        yield prefix + rule[offset:], None, None, (offset, None)


def _parse_route_rule(rule):
    pattern   = '' # regex with named groups
    factories = [] # factories to create value object
    nonames   = []
    for argname, mode, conf, seg in _parse_tokens(rule):
        print('argname=', argname)
        if mode:
            if mode in _route_rule_filters:
                mask, in_filter, out_filter = _route_rule_filters[mode](conf)
            else:
                mask = mode # default is regex expression
                out_filter = str


            if not argname:
                nonames.append(seg)

            if argname is None:
                pattern += '(%s)' % mask
            else:
                pattern += '(?P<%s>%s)' % (argname, mask)
            factories.append((argname, out_filter))
        elif argname:
            print(argname, re.escape(argname))
            # pattern += re.escape(argname)
            pattern += argname

    # if nonames:
    #     errmsg = "These arguments should be named regex: "
    #     errmsg += ', '.join([
    #         '[%d:%d](%s)' % (seg[0], seg[1], rule[seg[0]:seg[1]])
    #         for seg in nonames])
    #     raise ValueError(errmsg)

    return pattern, dict(factories)
