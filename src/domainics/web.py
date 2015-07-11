# -*- coding: utf-8 -*- 

import re
import os.path
import inspect
import pkgutil
import importlib

import tornado.web
from tornado.log import access_log

import mimetypes

from .pillar import _pillar_history, pillar_class



class RouteError:
    pass

def route_base(path, methods=None, fields=None, proto=None):

    # get routes from the invoking module
    routes = _get_module_routes(inspect.currentframe().f_back)

    if methods is not None:
        for elem in _arg_elem_or_list(methods.upper()):
            routes.default_methods.append(elem)

    if path is not None:
        routes.base_path.append(path)

    if proto is not None:
        proto = proto.upper()
        assert proto.upper() in ('REST'), 'not supported protocols: ' + proto
        routes.default_proto = proto     

def route(path, methods=None, fields=None, proto=None):
    """
    :param methods: GET/POST/PUT/DELETE
    :param proto:   default is None, REST
    """

    def route_decorator(handler_func):
        request_handler_def(path, handler_func, methods, fields, proto)

    return route_decorator


def request_handler_def(path, handler_func, methods=None, fields=None, proto=None) :
    """

    :param path: path pattern
    :param handler_func: request handler function
    """
    if isinstance(methods, str):
        methods = methods.upper()

    methods     = _arg_elem_or_list(methods)
    qry_fields  = _arg_elem_or_list(fields)
    
    ptn = re.compile(path)
    if len(ptn.groupindex) != ptn.groups :
        raise RouteError('groups in regex must be all named')
    
    path_fields = set(ptn.groupindex.keys())

    module_routes = _get_module_routes(handler_func)
    if proto is None:
        proto = module_routes.default_proto

    handler_class = _make_handler_class(handler_func, proto, methods, path_fields, qry_fields)
    fullpath = os.path.join(*module_routes.base_path)
    fullpath = os.path.join(fullpath, path)

    module_routes.add_spec(fullpath, handler_class, None)




handler = pillar_class(tornado.web.RequestHandler)(_pillar_history)


def _make_handler_class(serv_func, proto, methods, path_fields, qry_fields):

    func_signature = inspect.signature(serv_func).parameters
    path_fields    = [f for f in func_signature if f in set(path_fields)]
    qry_fields     = [f for f in func_signature if f in set(qry_fields)]

    def handler_wrapper(func, qry_fields):
        def handler_func(self, **kwargs):
            nonlocal func
            kwargs.update([(n, self.get_argument(n, None)) for n in qry_fields])

            self._handler_args = kwargs
            
            def exit_callback(exc_type, exc_val, tb):
                self._handler_args = None

            func = _pillar_history.bound(func, [(handler, self)], exit_callback)
            ret = func(**kwargs)

        return handler_func

    typename      = serv_func.__name__ + '_handler'
    handler_name  = serv_func.__module__ + '.' + serv_func.__name__

    def format_handler_class(cls):
        segs = ['<func \'', handler_name, 
                '(', ','.join(cls.path_fields + cls.query_fields), 
                ')\'>']
        return ''.join(segs)

    def format_handler(self):
        kwargs = self._handler_args or {}
        path_segs  = ['f=%s' % (f, str(kwargs.get(f, None))) for f in self.path_fields]
        query_segs = ['f=%s' % (f, str(kwargs.get(f, None))) for f in self.query_fields]
        segs = [handler_name, '(',  ','.join(path_fields + query_fields), ')']
        return ''.join(segs)


    # build the handler class template
    cls_tmpl = ['class ', typename ,'(baseclass):\n']
    for method in methods:
        cls_tmpl += ['  ', method.lower(), ' = wrapper(serv_func, qry_fields)\n']
        cls_tmpl += ['  ', 'handler_name = handler_name\n']
        cls_tmpl += ['  ', 'query_fields = qry_fields\n']
        cls_tmpl += ['  ', 'path_fields  = path_fields\n']
        cls_tmpl += ['  ', 'format_class = classmethod(fmt_class)\n']
        cls_tmpl += ['  ', '__str__      = fmt_handler\n']
    cls_tmpl = ''.join(cls_tmpl)

    namespace = dict(__name__     = 'tornice_' + typename, 
                     baseclass    = tornado.web.RequestHandler,
                     wrapper      = handler_wrapper, 
                     handler_name = handler_name,
                     serv_func    = serv_func, 
                     qry_fields   = qry_fields,
                     path_fields  = path_fields,
                     fmt_class    = format_handler_class,
                     fmt_handler  = format_handler)

    exec(cls_tmpl, namespace)
    result = namespace[typename]
    result.__module__ = serv_func.__module__

    return result


class TorniceRoutes:
    def __init__(self, place):
        self.proto     = 'HTTP'
        self.handlers  = []
        self.place     = place
        self.base_path = []

    def add_spec(self, pattern, handler, params=None):
        self.handlers.append((pattern, handler, params))

class WebApp:

    def __init__(self, **settings):
        self._request_handlers = []
        self.settings = settings

        main_module = inspect.getmodule(inspect.currentframe().f_back.f_code)
        self.add_handler_module(main_module)
    
    
    def add_static_handler(self, url_path, folder=None, index='index.html', default=None):
        """

        :param static_folder:
        :param url_path: URL path pattern
        :param folder:
        :param index: the index file in URL path
        :param default: the default path if the url path is not accessible.
        """

        routes = self._request_handlers
        params = dict(folder        = folder, 
                      index_file    = index, 
                      default_path  = default)

        routes.append((url_path, StaticFileHandler, params))


    def add_handler_module(self, pkg_or_module, submodules=True):

        for module in _enumerate_submodules(pkg_or_module, submodules):
            for path, handler, settings in module._tornice_routes.handlers:
                self._request_handlers.append((path, handler, settings))   

    def setup(self):
        """setup web application"""
        
        torapp = tornado.web.Application(self._request_handlers, **self.settings)
        torapp.listen(self.settings['port'])

        for url_ptn, handler, settings  in self._request_handlers: 
            print(url_ptn, handler.format_class(), settings)


def _get_module_routes(obj):
    func_module = inspect.getmodule(obj) 
    if not hasattr(func_module, '_tornice_routes'):
        module_routes = TorniceRoutes(func_module)
        setattr(func_module, '_tornice_routes', module_routes)
    else:
        module_routes = func_module._tornice_routes

    return module_routes

class StaticFileHandler(tornado.web.StaticFileHandler):
    '''read static files from folder.

    :param index_file:   The index file of folder
    :param default_path: Redirect to default path 
                         if path be inaccessible and the mime type of 
                         path are same with default_path
    '''
    
    def initialize(self, folder, index_file='index.html', default_path=None, debug=True):
        self.root         = folder
        self.debug        = debug
        self.index_file   = index_file
        self.default_path = default_path

    def set_extra_headers(self, path):
        if self.debug:
            self.set_header("Cache-control", "no-cache")

    text_mimetypes = ('text/html','text/css','application/javascript')
    
    def get_content_type(self):
        """make sure the default encoding is UTF-8"""
        mimetype, encoding = mimetypes.guess_type(self.absolute_path)

        if encoding is None and mimetype in self.text_mimetypes :
            mimetype = mimetype +  '; charset=UTF-8'

        return mimetype
        
    def validate_absolute_path(self, root_folder, absolute_path):
        """overload this method to handle default rule"""

        root_folder = os.path.abspath(root_folder)
        if not absolute_path.startswith(root_folder):
            errmsg = "%s is not in root_folder static directory" % self.path
            raise tornado.web.HTTPError(403, errmsg)
        
        if (os.path.isdir(absolute_path) and self.index_file is not None):
            if not self.request.path.endswith("/"):
                # if the path is folder, the path should end with '/'.
                self.redirect(self.request.path + "/", permanent=True)
                return
            absolute_path = os.path.join(absolute_path, self.index_file)

        if os.path.exists(absolute_path):
            if os.path.isfile(absolute_path):
                return absolute_path
            else:
                msg = "it is not a file: '%s' '%s' " % (absolute_path, self.path)
                raise tornado.web.HTTPError(403, msg)

        if self.request.path == self.default_path: # default_path does exists
            errmsg = 'NOT FOUND: default_path[%s]: %s ' 
            errmsg %= (self.default_path, absolute_path)
            access_log.warn(errmsg)
            raise tornado.web.HTTPError(404)

        if self.default_path is not None:
            guess_mimetype = lambda path : mimetypes.guess_type(path)[0]

            abs_default_path = os.path.join(root_folder, self.default_path)
            abs_default_path = os.path.abspath(abs_default_path)

            if guess_mimetype(abs_default_path) == guess_mimetype(absolute_path) :
                # redirect if the mime type of path are same with default_path 
                self.redirect(self.default_path)
        else:
            raise tornado.web.HTTPError(404)

    @classmethod
    def format_class(cls):
        return str(cls)


def _arg_elem_or_list(arg):
    """cast element list arg into tuple arg

    :param arg: 'GET POST', 'GET, POST' or  ['GET', 'POST']
    """
    if isinstance(arg, tuple) or isinstance(arg, list):
        return arg 
    return tuple(s.strip() for s in arg.replace(',', ' ').split())    


def _enumerate_submodules(pkg_or_module, submodules=True):
    if isinstance(pkg_or_module, str):
        pkg_or_module = importlib.import_module(pkg_or_module)
        if hasattr(pkg_or_module, '_tornice_routes'):
            yield pkg_or_module

    if not submodules:
        return

    if hasattr(pkg_or_module, '__path__'):
        module_prefix = pkg_or_module.__name__ + '.'
        for loader, module_name, ispkg in pkgutil.walk_packages(pkg_or_module.__path__, module_prefix):
            if ispkg: continue

            module = loader.find_module(module_name).load_module(module_name)
            if hasattr(module, '_tornice_routes'):
                yield module

