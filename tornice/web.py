# -*- coding: utf-8 -*- 

import re
import os.path
import inspect
import pkgutil
import importlib
import tornado.web

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




class StaticFileHandler(tornado.web.StaticFileHandler):
    '''read static files from folder.

    '''
    
    def initialize(self, folder, index_file=None, default_path=None, debug=True):
        self.root         = folder
        self.debug        = debug
        self.index_file   = index_file or 'index.html'
        self.default_path = default_path or '/index.html'

    def set_extra_headers(self, path):
        if self.debug:
            self.set_header("Cache-control", "no-cache")

    def get_content_type(self):
        """补充已知文件类型的字符编码，确定为UTF-8"""
        mime_type, encoding = mimetypes.guess_type(self.absolute_path)
        if encoding is None and mime_type in ('text/html', 'text/css',
                                              'application/javascript'
                                              ):
            mime_type = mime_type +  '; charset=UTF-8'
        return mime_type
        
    def validate_absolute_path(self, root, absolute_path):
        """overload this method to handle default rule"""

        print(root, absolute_path)
        return

        root = os.path.abspath(root)
        # os.path.abspath strips a trailing /
        # it needs to be temporarily added back for requests to root/
        if not (absolute_path + os.path.sep).startswith(root):
            raise tornado.web.HTTPError(403, "%s is not in root static directory",
                            self.path)
            
        if (os.path.isdir(absolute_path) and self.default is not None):
            # need to look at the request.path here for when path is empty
            # but there is some prefix to the path that was already
            # trimmed by the routing
            if not self.request.path.endswith("/"):
                self.redirect(self.request.path + "/", permanent=True)
                return
            absolute_path = os.path.join(absolute_path, self.default)

        if os.path.exists(absolute_path):
            if os.path.isfile(absolute_path):
                return absolute_path
            else:
                msg = "it is not a file: '%s' '%s' " % (absolute_path, self.path)
                raise HTTPError(403, msg)

        
        final_path = os.path.join(root, self.default)
        if mimetypes.guess_type(absolute_path)[0]:
            raise HTTPError(404) # It's concret file, which has mime type

        if not os.path.exists(final_path):
            raise HTTPError(404) 
        
        if not os.path.isfile(final_path):
            raise HTTPError(403, "%s is not a file", self.path)
        
        _logger.info("redirect: '%s' => '%s'", self.path, final_path)
        
        return final_path
    
    @classmethod
    def format_class(cls):
        return str(cls)

def _make_handler_class(serv_func, proto, methods, path_fields, qry_fields):

    func_signature = inspect.signature(serv_func).parameters
    path_fields    = [f for f in func_signature if f in set(path_fields)]
    qry_fields     = [f for f in func_signature if f in set(qry_fields)]

    def handler_wrapper(serv_func, qry_fields):
        def handler(self, **kwargs):
            kwargs.update([('request', self)])
            kwargs.update([(n, self.get_argument(n, None)) for n in qry_fields])

            try:
                self._handler_args = kwargs
                return serv_func(**kwargs)
            finally:
                self._handler_args = None

        return handler

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

    def __init__(self, app_name=None):
        self._request_handlers = []

        # add handlers defined in the module where the WebApp object are created
        routes = _get_module_routes(inspect.currentframe().f_back)
        for path, handler, settings in routes.handlers:
            self._request_handlers.append((path, handler, settings))  
    
    
    def add_static_handler(self, url_path, folder=None, index_file='index.html', default_path=None):
        """

        :param static_folder:
        :param url_path: URL path pattern
        :param folder:
        :param index: the index file in URL path
        :param default: the default path if the url path is not accessible.
        """

        routes = self._request_handlers
        settings = dict(folder        = folder, 
                        index_file    = index_file, 
                        default_path  = default_path)

        routes.append((url_path, StaticFileHandler, settings))


    def add_handler_module(self, pkg_or_module, submodules=True):

        for module in _enumerate_submodules(pkg_or_module, submodules):
            for path, handler, settings in module._tornice_routes.handlers:
                self._request_handlers.append((path, handler, settings))   

    def make(self):
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

