# -*- coding: utf-8 -*- 
import logging

import re
import os.path
import inspect
import pkgutil
import importlib

import tornado.web
from tornado.log import access_log

import mimetypes

from ..pillar import _pillar_history, pillar_class

from ..util import iter_submodules



class RouteError:
    pass


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

    route_table = _get_route_table(handler_func)
    if proto is None:
        proto = route_table.default_proto

    handler_class = _make_handler_class(handler_func, proto, methods, path_fields, qry_fields)
    fullpath = os.path.join(*route_table.base_path)
    fullpath = os.path.join(fullpath, path)

    route_table.set_rule(fullpath, handler_class, None)




_request_handler = pillar_class(tornado.web.RequestHandler)(_pillar_history)



from collections import OrderedDict
from .route import _parse_route_rule

class WebApp:

    def __init__(self, **settings):
        self._request_handlers = OrderedDict()
        self.settings = settings

        main_module = inspect.getmodule(inspect.currentframe().f_back.f_code)
        self.add_module(main_module)
    
    
    def add_static_handler(self, rule, folder=None, 
                           default=None, index='index.html'):
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

        path_ptn, pargs = _parse_route_rule(rule)
        self._request_handlers[rule] = (path_ptn, StaticFileHandler, params)


    def add_module(self, pkg_or_module):
        for module in iter_submodules(pkg_or_module):
            if not hasattr(module, '_module_route_table'):
                continue

            route_table = module._module_route_table
            for rule, ptn, hcls, params in route_table.handler_specs():
                if rule not in self._request_handlers:
                    self._request_handlers[rule] = (ptn, hcls, params)

    def setup(self):
        """setup web application"""

        handlers = []
        
        segs = ['Found request handlers:']
        for rule in self._request_handlers:
            path_ptn, hcls, params = self._request_handlers[rule]
            s = rule + ' {' + path_ptn +'} => ' + hcls.__name__ + '('
            if params:
                s += ', '.join(['%s=%r' % (k,v) for k, v in params.items()]) 
            s += ')'
            segs.append(s)
            handlers.append((path_ptn, hcls, params))


        self.logger.info('\n'.join(segs))

        if 'cookie_secret' not in self.settings:
            self.settings['cookie_secret'] = generate_cookie_secret()


        torapp = tornado.web.Application(handlers, **self.settings)
        torapp.listen(self.settings['port'])

        # for url_ptn, handler, settings  in self._request_handlers: 
        #     print(url_ptn, handler.format_class(), settings)



    def main(self):
        self.setup()
        import domainics.ioloop
        domainics.ioloop.run() # 服务主调度

    @property
    def logger(self):
        if hasattr(self, '_logger'):
            return self._logger

        logger = logging.getLogger(WebApp.__module__ +'.' + WebApp.__name__)
        setattr(self, '_logger', logger)
        
        return self._logger


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




# def _enumerate_submodules(pkg_or_module, submodules=True):
#     if isinstance(pkg_or_module, str):
#         pkg_or_module = importlib.import_module(pkg_or_module)
#         if hasattr(pkg_or_module, '_module_route_table'):
#             yield pkg_or_module

#     if not submodules:
#         return

#     if hasattr(pkg_or_module, '__path__'):
#         module_prefix = pkg_or_module.__name__ + '.'
#         for loader, module_name, ispkg in pkgutil.walk_packages(pkg_or_module.__path__, module_prefix):
#             if ispkg: continue

#             module = loader.find_module(module_name).load_module(module_name)
#             if hasattr(module, '_module_route_table'):
#                 yield module



import tempfile
import os
import stat
import uuid
import base64

def generate_cookie_secret(preserve=True):

    
    if preserve:
        tmpdir = os.path.join(tempfile.gettempdir(), 'tornice')
        os.makedirs(tmpdir, 0o700, exist_ok=True)

        secretfile = os.path.join(tmpdir, 'COOKIE_SECRET')
        if os.path.exists(secretfile):
            with open(secretfile, 'r') as f:
                cookie_secret = f.readline()
            os.chmod(secretfile, stat.S_IWUSR|stat.S_IRUSR)
            if cookie_secret is not None:
                return cookie_secret

    secret = base64.b64encode(uuid.uuid4().bytes + uuid.uuid4().bytes)
    secret = secret.decode('UTF-8')

    if secret:
        with open(secretfile, 'w') as f:
            f.write(secret)

    return secret