# -*- coding: utf-8 -*-

import logging
_logger = logging.getLogger(__name__)

import re
import inspect
import types
import datetime
import json
from collections import namedtuple, OrderedDict
from tornado.web import RequestHandler, HTTPError
from decimal import Decimal
from ..pillar import _pillar_history, pillar_class
from ..util   import comma_split, filter_traceback
from ..domobj import dset, dobject
from ..error  import AuthenticationError

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


def _http_delegate(handler, func, *args, **kwargs):
    return func(*args, **kwargs)

def _http_delegate_error(handler, status_code, **kwargs):
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
    path = handler.request.path
    errmsg = [
        '%d ERROR(%s): %s' % (status_code, exc_type.__name__, message),
        'This request (%s) was handled by %s' % (path, handler.handler_name)
    ]
    _logger.error('. '.join(errmsg), exc_info=True)

    for tb in tb_list:
        errmsg.append('    at %s, code: %s' % (tb['at'], tb['code']))
    errmsg = '\n'.join(errmsg)

    handler.set_status(status_code, reason=reason)
    handler.set_header('Content-Type', 'text/plain;charset=UTF-8')
    handler.write(errmsg)

    


def _json_rest_delegate(handler, func, *args, **kwargs):

    if 'jsonbody' in kwargs:
        body_data = handler.request.body
        if body_data : # if no body data, here is empty byte data
            kwargs['jsonbody'] = json.loads(body_data.decode('UTF-8'))
        else:
            kwargs['jsonbody'] = None

    obj = func(*args, **kwargs)
    if not isinstance(obj, (list, tuple, dset)):
        obj = [obj]

    handler.set_header('Content-Type', 'application/json')
    handler.write(json.dumps(obj, cls=_JSONEncoder))


def _json_rest_delegate_error(handler, status_code, **kwargs):
            
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

    path = handler.request.path
    errmsg = [
        '%d ERROR(%s): %s' % (status_code, exc_type.__name__, message),
        'This request (%s) was handled by %s' % (path, handler.handler_name)
    ]
    _logger.error('. '.join(errmsg), exc_info=True)

    errobj = OrderedDict([
            ('status_code', status_code),
            ('error', message),
            ('type', exc_type.__name__),
            ('path', path),
            ('handler', handler.handler_name),
            ('traceback', tb_list)
        ])

    handler.set_status(status_code, reason=reason)
    handler.set_header('Content-Type', 'application/json')
    handler.write(json.dumps([errobj], cls=_JSONEncoder))

class _JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, (Decimal)) :
            return float(obj)
        elif isinstance(obj, (dset, dobject)):
            return obj.export()
        else:
            return super(_JSONEncoder, self).default(obj)


proto_delegates = {
    'HTTP' : _http_delegate,
    'REST' : _json_rest_delegate
}

proto_error_delegates = {
    'HTTP' : _http_delegate_error,
    'REST' : _json_rest_delegate_error
}    

def _wrap_handler(func, proto, pargs, qargs):
    proto_delegate = proto_delegates[proto.upper()]

    nodefault_params = set()
    for param in inspect.signature(func).parameters.values():
        if param.default is param.empty:
            nodefault_params.add(param.name)

    def handler_func(self, *args, **kwargs):
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

        bound_func = _pillar_history.bound(func, 
                                           [(_request_handler_pillar, self)], 
                                           exit_callback)
        
        return proto_delegate(self, bound_func, *args, **kwargs)

    return handler_func

# def _str_handler_class(cls):
#     return '<func \'' + hndlname + '(' + ','.join(func_args) + ')\'>'

def _str_handler_object(self):
    kwargs = self._handler_args or {}
    segs = []
    for arg in func_args:
        segs.append('%s=%r' % (arg, kwargs.get(arg, None)))
    return hndlname + '(' + ', '.join(func_args) + ') at ' + hex(id(self)) 


class PrincipalProperty:
    """The principal of the authenticated session. 
    If it is unauthenticated, AuthenticationError is raised.

    If principal be set None or principal deleted, 
    it  will clear this authenticated session. 
    """

    _cookie_name = '_tornice_principal'

    def __get__(self, handler, owner):
        if handler._tornice_principal is not None:
            return handler._tornice_principal


        value = handler.get_secure_cookie(self._cookie_name)
        if value is not None:
            handler._tornice_principal = value
            return value.decode('utf-8')

        raise AuthenticationError('This session is unauthenticated')


    def __set__(self, handler, value):
        if value is not None and not isinstance(value, str):
            errmsg ='principal value should be str type, not ' 
            errmsg += '' 
            raise ValueError(errmsg)
        
        if value is None :
            handler.clear_cookie(self._cookie_name)
            handler._tornice_principal = None
            return 
        
        # session-only cookie, expires_days=None
        handler.set_secure_cookie(self._cookie_name, value, 
                                  expires_days=None, path='/', domain=None)  
        handler._tornice_principal = value    




def _make_handler_class(handler_func, proto, methods, pargs, qargs):

    func_sig    = inspect.signature(handler_func).parameters

    func_args = OrderedDict()
    for arg in func_sig:
        if arg in pargs:
            func_args[arg] = pargs[arg]
        elif arg in qargs:
            func_args[arg] = qargs[arg]
        else:
            func_args[arg] = None


    typename = handler_func.__name__ + '_handler'
    hndlname = handler_func.__module__ + '.' + handler_func.__name__

    delegate_func = _wrap_handler(handler_func, proto, pargs, qargs)
    delegate_err  = proto_error_delegates.get(proto)
    attrs = OrderedDict()
    for m in methods:
        attrs[m.lower()]  = delegate_func
    attrs['_tornice_principal'] = None
    attrs['__str__']      = _str_handler_object
    attrs['handler_name'] = hndlname
    attrs['principal']    = PrincipalProperty()
    if delegate_err:
        attrs['write_error'] = delegate_err
    
    newcls = type(typename, (RequestHandler,), attrs)


    return newcls
