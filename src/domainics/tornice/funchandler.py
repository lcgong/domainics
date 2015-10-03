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
from ..error  import AuthenticationError
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
        self._logger = logging.getLogger(self.service_name + '.handler')
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
        return self.service_name + '(' + ', '.join(kwargs) + ') at ' + hex(id(self))


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

    def parse_arguments(self, func_sig, args, kwargs):
        arguments = OrderedDict()

        for arg_name, arg_spec in func_sig.parameters.items():

            arg_val = None
            ann_type = arg_spec.annotation

            if arg_name in self.path_signature:
                # get argument value from path arguments
                arg_val = kwargs[arg_name]

            elif arg_name == 'json_arg':
                arg_val = self._read_json_object()

            else:
                arg_val = self.get_argument(arg_name, None)

            if ann_type != inspect._empty:
                print(77777, ann_type.__name__, )
                if issubclass(ann_type, DSet[DObject]):
                    # if ann_type.__origin__ == DSet[Any].__origin__:
                        # if len(ann_type.__parameters__) != 1:
                        #     errmsg = ("Argument %s's type is required "
                        #                "like DSet[T]")
                        #     errmsg %= arg_name
                        #     raise TypeError(errmsg)
                        #
                    item_type = ann_type.__parameters__[0]

                    if arg_name != 'json_arg' :
                        arg_val = self._read_json_object()

                    arg_val = dset(item_type)(arg_val)

                elif issubclass(ann_type, DObject):
                    if arg_name != 'json_arg' :
                        arg_val = self._read_json_object()

                    arg_val = ann_type(reshape(arg_val))
                else:
                    if issubclass(ann_type, datetime):
                        arg_val = arrow.get(arg_val).datetime

                    elif issubclass(ann_type, date):
                        arg_val = arrow.get(arg_val).datetime.date()

                    else:
                        arg_val = ann_type(arg_val)

            else:
                pass

            if arg_val is None and arg_spec.default is not inspect._empty :
                arg_val = param.default

            arguments[arg_name] = arg_val


            # if 'json_arg' == arg_name:
            #     # get json argument from body of http message
            #     arg_val = self.request.body
            #     # arg_val = self._read_json_object()
            #
            # elif arg_name in self.path_signature:
            #     # get argument value from path arguments
            #     # arg_type = self.path_signature[arg_name]
            #     arg_val = kwargs[arg_name]
            #     # if arg_type != str and arg_val is not None:
            #     #     arg_val = arg_type(arg_val)
            #     #
            #     # print('12', arg_name, arg_type)

            # elif arg_name in self.req_query_args:
            #     arg_type = self.req_query_args[arg_name]
            #     arg_val = self.get_argument(arg_name, None)
            #     if arg_type != str and arg_val is not None:
            #         arg_val = arg_type(arg_val)

            # elif arg_spec.annotation != inspect._empty:
            #
            #     ann = arg_spec.annotation
            #     if hasattr(ann, '__origin__'): # maybe DSet[DObject]
            #         if ann.__origin__ == DSet[Any].__origin__:
            #             if len(ann.__parameters__) != 1:
            #                 errmsg = ("Argument %s's type is required "
            #                            "like DSet[T]")
            #                 errmsg %= arg_name
            #                 raise TypeError(errmsg)
            #             item_type = ann.__parameters__[0]
            #             arg_val = dset(item_type, self._read_json_object())
            #
            #     elif issubclass(ann, DObject):
            #         arg_val = ann(reshape(self._read_json_object()))
            #     else:
            #         errmsg = "Unknow type hinting: %s"
            #         errmsg %= arg_sepc
            #         raise ValueError(errmsg)

            # else:
            #     if arg_spec.default is inspect._empty :
            #         arg_val = None
            #     else:
            #         arg_val = param.default
            #
            # arguments[arg_name] = arg_val

        return arguments


    def do_handler_func(self, *args, **kwargs) :

        func_sig = inspect.signature(self.service_func)

        arguments = self.parse_arguments(func_sig, args, kwargs)

        self._handler_args = arguments

        def exit_callback(exc_type, exc_val, tb):
            self._handler_args = None

        busilogic_layer = BusinessLogicLayer(self.service_name,
                                                self.principal_id)

        bound_func = _pillar_history.bound(self.service_func,
                                           [(_request_handler_pillar, self),
                                            (_busilogic_pillar,
                                             busilogic_layer)],
                                             exit_callback)

        result = bound_func(**arguments)

        # use type hinting
        ret_type = func_sig.return_annotation
        if result is not None and ret_type != inspect._empty:
            if ret_type == result.__class__ :
                return result
            else:
                return ret_type(reshape(result))

        return result


class RESTFuncRequestHandler(BaseFuncRequestHandler):


    def do_handler_func(self, *args, **kwargs):
        obj = super(RESTFuncRequestHandler, self).do_handler_func(*args, **kwargs)

        if not isinstance(obj, (list, tuple, DSetBase)):
            obj = [obj] if obj is not None else []

        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        self.write(_json.dumps(obj))


    def write_error(self, status_code, **kwargs):
        exc_info = kwargs['exc_info']
        status_code, reason, message, tb_list = self.handle_exception(exc_info)

        errobj = OrderedDict(
                    status_code=status_code,
                    message=message,
                    exception=exc_info[0].__name__,
                    path=self.request.path,
                    handler=self.service_name,
                    traceback=tb_list)

        self.set_status(status_code, reason=reason)
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        self.write(_json.dumps([errobj]))
