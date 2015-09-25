# -*- coding: utf-8 -*-

import logging
import tornado.ioloop
import tornado.web
import signal
import socket
import multiprocessing

class ApplicationServerProcess(multiprocessing.Process):
    """A process-based Simple application server in testing. """

    def __init__(self, application, host='localhost', port=None):
        self.application = application
        self.host = host

        self.port = port if port is not None else _get_unused_socket_port()

        self.home_url = 'http://%s:%d/' % (self.host, self.port)

        self.is_starting_event = multiprocessing.Event()

        super(ApplicationServerProcess, self).__init__()

    @property
    def logger(self):
        return logging.getLogger('server')

    def start(self, async=False):
        super(ApplicationServerProcess, self).start()

        if not async:
            self.is_starting_event.wait() # wait util starting is finished

    def run(self):
        try:
            ioloop = tornado.ioloop.IOLoop.instance()

            def handler(signum, frame):
                ioloop.add_callback(ioloop.stop)


            signal.signal(signal.SIGTERM, handler)

            from tornado.httpserver import HTTPServer
            http_server = HTTPServer(self.application)
            http_server.listen(self.port, address=self.host)
            self.logger.info('Application Server is Ready: ' +  self.home_url)

            def ready():
                self.is_starting_event.set()
            ioloop.add_callback(ready)

            ioloop.start()
        except:
            self.is_starting_event.set()
            raise


    def stop(self):
        self.terminate()
        self.join()

        self.logger.info('Server (%s) was down' % self.home_url)
        self.application = None

def _get_unused_socket_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('localhost', 0))
        addr, port = s.getsockname()
        return port
