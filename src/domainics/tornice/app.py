# -*- coding: utf-8 -*-
import logging

import re
import bisect
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


_request_handler = pillar_class(tornado.web.RequestHandler)(_pillar_history)



from collections import OrderedDict
from .route import _parse_route_rule

class OrderedURLSpec(tornado.web.URLSpec):

    def __init__(self, priority, pattern, handler,
                                            path=None, kwargs=None, name=None):
        self.priority = priority
        self.path = path
        super(OrderedURLSpec, self).__init__(pattern, handler, kwargs, name)

    def __lt__(self, other):
        if self.priority < other.priority:
            return True

        if self.priority == other.priority:
            if self.regex.pattern < other.regex.pattern:
                return True

        return False

class Application(tornado.web.Application):

    def __init__(self, **settings):
        self._handlers = OrderedDict()
        self.settings = settings

        if 'cookie_secret' not in self.settings:
            self.settings['cookie_secret'] = generate_cookie_secret()

        super(Application, self).__init__(**settings)

    def add_static_handler(self, pattern, folder=None,
                           default=None, index='index.html', priority=None):
        """
        :param static_folder:
        :param url_path: URL path pattern
        :param folder:
        :param index: the index file in URL path
        :param default: the default path if the url path is not accessible.
        """

        kwargs = dict(folder        = folder,
                      index_file    = index,
                      default_path  = default)

        self._add_handler(pattern, StaticFileHandler,
                            kwargs=kwargs, priority=100)

    def add_module(self, pkg_or_module, priority=50):
        for module in iter_submodules(pkg_or_module):

            if not hasattr(module, '__http_route_spec_table__'):
                continue


            route_table = module.__http_route_spec_table__
            route_table.setup()

            for spec in route_table.route_specs:
                self._add_handler(spec.path_pattern, spec.handler_class,
                                            priority=priority, path=spec.path)


    def log_route_handlers(self):
        """show request handler specifictions"""
        if not self.handlers:
            self.logger.warn('No defined request handlers')
            return

        host_pattern, handlers = self.handlers[0]
        assert host_pattern.pattern == '.*$'

        segs = ['Found %d request handlers:' % len(handlers)]
        for spec in handlers:
            if spec.kwargs:
                kwargs_expr = ', '.join(['%s=%r' % (k,v)
                                            for k, v in spec.kwargs.items()])
            else:
                kwargs_expr = ''

            hcls = spec.handler_class
            assert hcls is not None
            if spec.path:
                s = "%s {%s} => %s(%s)"
                s %= (spec.path, spec.regex.pattern, hcls.__name__, kwargs_expr)
            else:
                s = "%s => %s(%s)"
                s %= (spec.regex.pattern, hcls.__name__, kwargs_expr)

            segs.append(s)

        self.logger.info('\n'.join(segs))


    def _add_handler(self, pattern, handler_class,
                                        path=None, kwargs=None, priority=100):

        if not self.handlers:
            host_pattern = re.compile(r'.*$')
            handlers = []
            self.handlers.append((host_pattern, handlers))
        else:
            host_pattern, handlers = self.handlers[0]
            assert host_pattern.pattern == '.*$'

        assert pattern is not None and handler_class is not None

        urlspec = OrderedURLSpec(priority, pattern, handler_class,
                                                    path=path, kwargs=kwargs)
        bisect.insort_left(handlers, urlspec)


    def run(self, port=None, host=None):
        logger.info('DDDD')

        if port is not None:
            self.settings['port'] = port

        if host is not None:
            self.settings['host'] = host

        self.log_route_handlers()

        self.add_handlers(".*$", self._handlers)
        self.listen(self.settings['port'])


        import domainics.ioloop
        domainics.ioloop.run() # 服务主调度

    @property
    def logger(self):
        if hasattr(self, '_logger'):
            return self._logger

        self._logger = logging.getLogger('webapp')
        setattr(self, '_logger', self._logger)

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
