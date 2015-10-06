# -*- coding: utf-8 -*-

import tornado.web


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
