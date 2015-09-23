# -*- coding: utf-8 -*-

import logging

import re
import inspect
import types
import datetime
import functools
from collections import namedtuple, OrderedDict
from tornado.web import RequestHandler, HTTPError
from decimal import Decimal
from ..pillar import _pillar_history, pillar_class
from ..util   import comma_split, filter_traceback
from ..domobj import dset, dobject
from ..error  import AuthenticationError
from .. import json as _json

from ..busitier import _busilogic_pillar, BusinessLogicLayer

from .funchandler import BaseFuncRequestHandler, RESTFuncRequestHandler


def route_base(rule_base, method=None, qargs=None):

    # get routes from the invoking module
    route_table = _get_route_table(inspect.currentframe().f_back)
    route_table.rule_base = rule_base
    route_table.method = method
    route_table.qargs  = qargs


def service(path, method=None, qargs=None):
    """
    :param method: GET/POST/PUT/DELETE
    """
    def route_decorator(func):
        route_table = _get_route_table(func)
        route_table.set_rule(path, qargs, method, func, None, 'REST')

        return func

    return route_decorator

rest_route = service

def http_route(rule, method=None, qargs=None):
    """
    :param method: GET/POST/PUT/DELETE
    """

    def route_decorator(func):
        route_table = _get_route_table(func)
        route_table.set_rule(rule, qargs, method, func, None, 'HTTP')

        return func

    return route_decorator


class ModuleRouteTable:
    def __init__(self, module):
        self.module    = module
        self.method    = set()
        self.proto     = 'HTTP'
        self.handlers  = OrderedDict()
        self.rule_base = ''

    def set_rule(self, rule, qargs, method, handler_func, params=None, proto=None):
        if rule in self.handlers:
            errmsg = 'The rule %s has already set in %s '
            errmsg %= (rule, route_table.module.__name__)
            raise NameError(errmsg)
        self.handlers[rule] = RouteSpec(rule, qargs, method, proto,
                                        handler_func, params)

    def handler_specs(self):

        for rule, spec in self.handlers.items():
            route_rule     = self.rule_base + rule
            pattern, pargs = _parse_route_rule(route_rule)

            qargs = OrderedDict()
            if spec.qargs is not None:
                for arg in comma_split(spec.qargs):
                    qargs[arg] = str

            methods = comma_split(spec.method if spec.method else self.method)
            proto   = spec.proto if spec.proto else self.proto

            print(spec.handler)

            handler_cls = _make_handler_class(spec.handler,
                                              spec.proto,
                                              methods,
                                              pargs,
                                              qargs)

            yield route_rule, pattern, handler_cls, spec.params



RouteSpec = namedtuple('RouteSpec',
    ['rule', 'qargs', 'method', 'proto', 'handler', 'params'])

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


def _get_route_table(obj):
    handler_module = inspect.getmodule(obj)
    if not hasattr(handler_module, '_module_route_table'):
        route_table = ModuleRouteTable(handler_module)
        setattr(handler_module, '_module_route_table', route_table)
    else:
        route_table = handler_module._module_route_table

    return route_table


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
            pattern += re.escape(argname)

    # if nonames:
    #     errmsg = "These arguments should be named regex: "
    #     errmsg += ', '.join([
    #         '[%d:%d](%s)' % (seg[0], seg[1], rule[seg[0]:seg[1]])
    #         for seg in nonames])
    #     raise ValueError(errmsg)

    return pattern, dict(factories)


def _make_handler_class(service_func, proto, methods, pargs, qargs):

    if not inspect.isfunction(service_func):
        errmsg = "The service function '%s' in '%s' should be a function"
        errmsg %= (service_func.__name__, service_module.__name__)
        raise TypeError(errmsg)

    service_module = inspect.getmodule(service_func)
    try:
        # To be decorated-order irrevalent!!!
        # There are decorators will decorator this function laterly.
        # To make these decorators available, refresh these service function
        service_func = getattr(service_module, service_func.__name__)
    except AttributeError as ex:
        errmsg = "Could not find '%s' service function in module '%s'. "
        errmsg %= (service_func.__name__, service_module.__name__)
        errmsg += "The function or its name shoule be declared in module."
        raise AttributeError(errmsg)


    attrs = dict(
        # service_func is function that has no 'self' parameter
        handler_func    = staticmethod(service_func),
        handler_name    = service_module.__name__ + '.' + service_func.__name__,
        req_path_args   = pargs,
        req_query_args  = qargs
        )

    if proto == 'REST':
        base_class = RESTFuncRequestHandler
    elif proto == 'HTTP':
        base_class = BaseFuncRequestHandler

    cls = type(service_func.__name__ + '_handler', (base_class,), attrs)
    for m in methods:
        setattr(cls, m.lower(), cls.do_handler_func)

    return cls
