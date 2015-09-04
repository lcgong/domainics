# -*- coding: utf-8 -*-

import logging
_logger = logging.getLogger(__name__)

import re
import inspect
import types
import datetime
from collections import namedtuple, OrderedDict
from tornado.web import RequestHandler, HTTPError
from decimal import Decimal
from ..pillar import _pillar_history, pillar_class
from ..util   import comma_split, filter_traceback
from ..domobj import dset, dobject
from ..error  import AuthenticationError
from .. import json as _json

from ..business import _busilogic_pillar, BusinessLogicLayer



_request_handler_pillar = pillar_class(RequestHandler)(_pillar_history)

def route_base(rule_base, method=None, qargs=None):

    # get routes from the invoking module
    route_table = _get_route_table(inspect.currentframe().f_back)
    route_table.rule_base = rule_base
    route_table.method = method
    route_table.qargs  = qargs


    # if methods is not None:
    #     for elem in _arg_elem_or_list(methods.upper()):
    #         routes.default_methods.append(elem)

    # if path is not None:
    #     routes.base_path.append(path)

    # if proto is not None:
    #     proto = proto.upper()
    #     assert proto.upper() in ('REST'), 'not supported protocols: ' + proto
    #     routes.default_proto = proto     

def rest_route(rule, method=None, qargs=None):
    """
    :param method: GET/POST/PUT/DELETE
    """
    def route_decorator(func):
        route_table = _get_route_table(func)
        route_table.set_rule(rule, qargs, method, func, None, 'REST')
        
        return func

    return route_decorator

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
            for arg in comma_split(spec.qargs):
                qargs[arg] = str

            methods = comma_split(spec.method if spec.method else self.method)
            proto   = spec.proto if spec.proto else self.proto

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


class BaseRequestHandler(RequestHandler):

    @property
    def logger(self):
        if hasattr(self, '_logger'):
            return self._logger
        self._logger = logging.getLogger(self.handler_name + '.request')
        return self._logger


    @property
    def principal_id(self):
        if hasattr(self, '__tornice_principal'):
            return getattr(self, '__tornice_principal')

        value = self.get_secure_cookie('tornice_principal')
        if value is not None:
            value = value.decode('utf-8')
        setattr(self, '__tornice_principal', value)

        return value

    @principal_id.setter
    def principal_id(self, value):
        if value is not None and not isinstance(value, str):
            raise TypeError('principal_id should be str or None ' )
        
        if value is None :
            self.clear_cookie(self._cookie_name)
        else:        
            # session-only cookie, expires_days=None
            self.set_secure_cookie('tornice_principal', value, 
                                      expires_days=None, path='/', domain=None)  
            
        delattr(self, '__tornice_principal')

    def __str__(self):
        kwargs = self._handler_args or {}
        segs = []
        for arg in kwargs:
            segs.append('%s=%r' % (arg, kwargs.get(arg, None)))
        return self.handler_name + '(' + ', '.join(kwargs) + ') at ' + hex(id(self)) 


    def write_error(self, status_code, **kwargs):
        exc_type, exc_val, exc_tb = kwargs['exc_info']

        if isinstance(exc_val, HTTPError):
            status_code = exc_val.status_code
            reason = exc_val.reason if hasattr(exc_val, 'reason') else None
            message = exc_val.log_message
        else:
            status_code = 500
            reason='Server Exception'
            message = str(exc_val)
            print(messagee)
        
        tb_list = filter_traceback(exc_tb, excludes=['domainics.', 'tornado.'])
        path = self.request.path
        errmsg = [
            '%d ERROR(%s): %s' % (status_code, exc_type.__name__, message),
            'This request (%s) was handled by %s' % (path, self.handler_name)
        ]
        _logger.error('. '.join(errmsg), exc_info=True)

        for tb in tb_list:
            errmsg.append('    at %s, code: %s' % (tb['at'], tb['code']))
        errmsg = '\n'.join(errmsg)

        self.set_status(status_code, reason=reason)
        self.set_header('Content-Type', 'text/plain;charset=UTF-8')
        self.write(errmsg)

    def do_handler_func(self, *args, **kwargs) :

        nodefault_params = set()
        for param in inspect.signature(self.handler_func).parameters.values():
            if param.default is param.empty:
                nodefault_params.add(param.name)        

        pargs = self.req_path_args
        qargs = self.req_query_args
        for arg in kwargs:
            if arg in pargs and pargs[arg] != str:
                kwargs[arg] = pargs[arg](kwargs[arg])
            elif arg in qargs:                
                kwargs[arg] = self.get_argument(n, None)

        for param in nodefault_params: # the param is not assigned
            if param not in kwargs:
                kwargs[param] = None

        self._handler_args = kwargs
        
        def exit_callback(exc_type, exc_val, tb):
            self._handler_args = None


        busilogic_layer = BusinessLogicLayer(self.handler_name, self.principal_id)

        bound_func = _pillar_history.bound(self.handler_func, 
                                           [(_request_handler_pillar, self),
                                           (_busilogic_pillar, busilogic_layer)], 
                                           exit_callback)
        
        return bound_func(*args, **kwargs)


class RESTRequestHandler(BaseRequestHandler):


    def do_handler_func(self, *args, **kwargs):

        if 'jsonbody' in kwargs:
            body_data = self.request.body
            if body_data : # if no body data, here is empty byte data
                kwargs['jsonbody'] = _json.loads(body_data.decode('UTF-8'))
            else:
                kwargs['jsonbody'] = None

        obj = super(RESTRequestHandler, self).do_handler_func(*args, **kwargs)

        # obj = func(*args, **kwargs)
        if not isinstance(obj, (list, tuple, dset)):
            obj = [obj]

        self.set_header('Content-Type', 'application/json')
        self.write(_json.dumps(obj))


    def write_error(self, status_code, **kwargs):
        exc_type, exc_val, exc_tb = kwargs['exc_info']

        if isinstance(exc_val, HTTPError):
            status_code = exc_val.status_code
            reason = exc_val.reason if hasattr(exc_val, 'reason') else None
            message = exc_val.log_message
        else:
            status_code = 500
            reason='Server Exception'
            message = str(exc_val)


        tb_list = filter_traceback(exc_tb, excludes=['domainics.', 'tornado.'])

        errmsg = '%s[%d, %s]: %s' 
        errmsg %= (exc_type.__name__, status_code, self.request.path, message)
        self.logger.error(errmsg, exc_info=kwargs['exc_info'])

        errobj = OrderedDict([
                ('status_code', status_code),
                ('error', message),
                ('type', exc_type.__name__),
                ('path', self.request.path),
                ('handler', self.handler_name),
                ('traceback', tb_list)
            ])

        self.set_status(status_code, reason=reason)
        self.set_header('Content-Type', 'application/json')
        self.write(_json.dumps([errobj]))




def _make_handler_class(func, proto, methods, pargs, qargs):
    
    attrs = dict(
        # func is function that has no 'self' parameter
        handler_func    = staticmethod(func), 
        handler_name    = func.__module__ + '.' + func.__name__,
        req_path_args   = pargs,
        req_query_args  = qargs
        )
    
    
    if proto == 'REST':
        base_class = RESTRequestHandler
    elif proto == 'HTTP':
        base_class = BaseRequestHandler

    cls = type(func.__name__ + '_handler', (base_class,), attrs)
    for m in methods:
        setattr(cls, m.lower(), cls.do_handler_func) 
    
    return cls

