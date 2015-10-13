# -*- coding: utf-8 -*-

import logging
import tornado.web
import os
import mimetypes


class RaiseErrorHandler(tornado.web.RequestHandler):

    def initialize(self, status_code=None, reason=None):
        self.status_code = status_code
        self.reason = reason

    def get(self, *args):
        raise tornado.web.HTTPError(self.status_code, self.reason)

    def put(self, *args):
        raise tornado.web.HTTPError(self.status_code, self.reason)

    def post(self, *args):
        raise tornado.web.HTTPError(self.status_code, self.reason)

    def delete(self, *args):
        raise tornado.web.HTTPError(self.status_code, self.reason)


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

        if self.default_path is not None and self.default_path[0] == '/':
            self.default_path = self.default_path[1:]

    @property
    def logger(self):
        if hasattr(self, '_logger'):
            return self._logger

        self._logger = logging.getLogger('static_handler')
        return self._logger

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
            errmsg = "There is no %s in root_folder static directory"
            errmsg %= self.path
            raise tornado.web.HTTPError(403, errmsg)

        if os.path.isfile(absolute_path):
            return absolute_path

        if (os.path.isdir(absolute_path) and self.index_file is not None):
            file_path = os.path.join(absolute_path, self.index_file)
            if os.path.isfile(file_path):
                return file_path

        if self.default_path is not None:
            # guess_mimetype = lambda path : mimetypes.guess_type(path)[0]

            file_path = os.path.join(root_folder, self.default_path)
            file_path = os.path.abspath(file_path)

            request_mimetype = mimetypes.guess_type(absolute_path)[0]
            if request_mimetype is None:
                return file_path


        self.logger.debug('not found: %s' % absolute_path)
        raise tornado.web.HTTPError(404)

    @classmethod
    def format_class(cls):
        return str(cls)
