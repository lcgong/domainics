# -*- coding: utf-8 -*-

import logging

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

from ..busitier import _busilogic_pillar, BusinessLogicLayer


_request_handler_pillar = pillar_class(RequestHandler)(_pillar_history)

webreq = _request_handler_pillar

class BaseFuncRequestHandler(RequestHandler):

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
        self.logger.error('. '.join(errmsg), exc_info=kwargs['exc_info'])

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


class RESTFuncRequestHandler(BaseFuncRequestHandler):


    def do_handler_func(self, *args, **kwargs):

        if 'json_arg' in kwargs:
            body_data = self.request.body
            if body_data : # if no body data, here is empty byte data
                kwargs['json_arg'] = _json.loads(body_data.decode('UTF-8'))
            else:
                kwargs['json_arg'] = None

        obj = super(RESTFuncRequestHandler, self).do_handler_func(*args, **kwargs)

        # obj = func(*args, **kwargs)
        if not isinstance(obj, (list, tuple, dset)):
            obj = [obj]

        self.set_header('Content-Type', 'application/json; charset=UTF-8')
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


