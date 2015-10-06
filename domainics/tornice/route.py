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
from .. import json as _json

from urllib.parse import urljoin

from ..busitier import _busilogic_pillar, BusinessLogicLayer

from .funchandler import BaseFuncRequestHandler, RESTFuncRequestHandler

from .route_path import parse_route_path


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
        path_pattern, path_args = parse_route_path(route_spec.path)
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
