# -*- coding: utf-8 -*-

import logging

import re
import inspect
import types
import arrow
from datetime import datetime, date
from collections import namedtuple, OrderedDict
from tornado.web import RequestHandler, HTTPError
from decimal import Decimal
from ..pillar import _pillar_history, pillar_class
from ..util   import comma_split, filter_traceback
from ..domobj import dset, dobject, DSetBase, DObject, DSet

from ..domobj.pagination import DPage, parse_query_range, parse_header_range

from .. import json as _json

from typing import Any

from ..busitier import _busilogic_pillar, BusinessLogicLayer

from ..exception import UnauthorizedError, ForbiddenError, BusinessLogicError

_request_handler_pillar = pillar_class(RequestHandler)(_pillar_history)

webreq = _request_handler_pillar

class BaseFuncRequestHandler(RequestHandler):

    @property
    def logger(self):
        if hasattr(self, '_logger'):
            return self._logger
        self._logger = logging.getLogger(self.__class__.__name__)
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
            self.clear_cookie('tornice_principal')
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
        return self.__class__.__name__ + '(' + ', '.join(kwargs) + ') at ' + hex(id(self))


    def handle_exception(self, exc_info):
        exc_type, exc_val, exc_tb = exc_info

        if isinstance(exc_val, HTTPError):
            status_code = exc_val.status_code
            reason = exc_val.reason if hasattr(exc_val, 'reason') else None
            message = exc_val.log_message
        elif isinstance(exc_val, UnauthorizedError):
            status_code = 401
            reason = 'Unauthorized'
            message = str(exc_val)
        elif isinstance(exc_val, ForbiddenError):
            status_code = 403
            reason = 'Forbidden'
            message = str(exc_val)
        elif isinstance(exc_val, BusinessLogicError):
            status_code = 409
            reason = 'Conflict(Business Logic)'
            message = str(exc_val)
        else:
            status_code = 500
            reason='Internal Server Error'
            message = str(exc_val)

        tb_list = filter_traceback(exc_tb, excludes=['domainics.', 'tornado.'])

        # errmsg = '%s[%d, %s]: %s'
        # errmsg %= (exc_type.__name__, status_code, self.request.path, message)
        # self.logger.error(errmsg, exc_info=exc_info)

        return status_code, reason, message, tb_list

    def write_error(self, status_code, **kwargs):
        exc_info = kwargs['exc_info']
        status_code, reason, message, tb_list = self.handle_exception(exc_info)

        errmsg = 'ERROR %d: %s\nCaught an exception %s\n'
        errmsg %= (status_code, message, exc_type.__name__)
        for tb in tb_list:
            errmsg += '    at %s, code: %s\n' % (tb['at'], tb['code'])

        self.set_status(status_code, reason=reason)
        self.set_header('Content-Type', 'text/plain; charset=UTF-8')
        self.write(errmsg)

    def _read_json_object(self):
        body_data = self.request.body
        if body_data : # if no body data, here is empty byte data
            return _json.loads(body_data.decode('UTF-8'))

        return None

def parse_arguments(handler, service_func, path_signature, args, kwargs):
    arguments = OrderedDict()

    func_sig = inspect.signature(service_func)

    for arg_name, arg_spec in func_sig.parameters.items():

        arg_val = None
        ann_type = arg_spec.annotation

        if arg_name in path_signature:
            # get argument value from path arguments
            arg_val = kwargs[arg_name]

        elif arg_name == 'json_arg':
            arg_val = handler._read_json_object()

        else:
            arg_val = handler.get_argument(arg_name, None)

        if ann_type != inspect._empty:
            if issubclass(ann_type, DSet[DObject]):

                item_type = ann_type.__parameters__[0]

                if arg_name != 'json_arg' :
                    arg_val = handler._read_json_object()

                arg_val = dset(item_type)(arg_val)

            elif issubclass(ann_type, DObject):
                if arg_name != 'json_arg' :
                    arg_val = handler._read_json_object()

                arg_val = ann_type(arg_val)
            elif issubclass(ann_type, DPage):
                arg_val = make_pagination(handler)

            else:
                if issubclass(ann_type, datetime):
                    arg_val = arrow.get(arg_val).datetime

                elif issubclass(ann_type, date):
                    arg_val = arrow.get(arg_val).datetime.date()

                else:
                    # if arg_val is None:
                    #     return None
                    # else:
                    try:
                        if arg_val is not None:
                            arg_val = ann_type(arg_val)
                    except TypeError as exc :
                        errmsg = ("while parsing arg '%s' of %s, "
                                  "caught an exception: ")
                        errmsg %= (arg_name, service_func.__name__)
                        errmsg += str(exc)
                        raise TypeError(errmsg)

        else:
            pass

        if arg_val is None and arg_spec.default is not inspect._empty :
            arg_val = param.default

        arguments[arg_name] = arg_val

    return arguments


def make_pagination(handler):

    start = None
    limit = None
    sortable = None

    content_range = handler.get_argument('range', None)
    if content_range:
        start, limit, sortable = parse_query_range(content_range)

    content_range = handler.request.headers.get("Range", None)
    if content_range :
        start, limit, total, sortable = parse_header_range(content_range)

    page = DPage(start=start, limit=limit, sortable=sortable)

    return page

def service_func_handler(proto, service_func, service_name, path_sig) :

    def http_handler(self, *args, **kwargs):

        setattr(self, 'service_name', service_name)

        func_sig = inspect.signature(service_func)

        arguments = parse_arguments(self, service_func, path_sig, args, kwargs)

        self._handler_args = arguments

        def exit_callback(exc_type, exc_val, tb):
            self._handler_args = None

        busilogic_layer = BusinessLogicLayer(service_name, self.principal_id)

        bound_func = _pillar_history.bound(service_func,
                                           [(_request_handler_pillar, self),
                                            (_busilogic_pillar,
                                             busilogic_layer)],
                                             exit_callback)

        result = bound_func(**arguments)

        # use type hinting
        ret_type = func_sig.return_annotation
        if result is not None and ret_type != inspect._empty:

            if issubclass(ret_type, DSet[DObject]):
                if isinstance(result, DSetBase):
                    return result
                else:
                    item_type = ret_type.__parameters__[0]
                    result = dset(item_type)([result])
                    return result

            elif issubclass(result.__class__, ret_type) :
                return result
            else:
                return ret_type(result)

        return result

    def rest_handler(self, *args, **kwargs):
        obj = http_handler(self, *args, **kwargs)

        if not isinstance(obj, (list, tuple, DSetBase)):
            obj = [obj] if obj is not None else []

        if isinstance(obj, DSetBase) and hasattr(obj, '_page'):
            content_range = obj._page.format_content_range()
            self.set_header('Content-Range', content_range)
            if obj._page.start != 0 or obj._page.limit is not None:
                self.set_status(206)

        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        self.write(_json.dumps(obj))

    if proto == 'REST':
        return rest_handler
    elif proto == 'HTTP':
        return http_handler
    else:
        raise ValueError('Unknown')



class RESTFuncRequestHandler(BaseFuncRequestHandler):


    def write_error(self, status_code, **kwargs):
        exc_info = kwargs['exc_info']
        status_code, reason, message, tb_list = self.handle_exception(exc_info)

        if hasattr(self, 'service_name'):
            service_name = self.service_name
        else:
            service_name = 'unknown_handler'

        errobj = OrderedDict(
                    status_code=status_code,
                    message=message,
                    exception=exc_info[0].__name__,
                    path=self.request.path,
                    handler=service_name,
                    traceback=tb_list)

        self.set_status(status_code, reason=reason)
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        self.write(_json.dumps([errobj]))
