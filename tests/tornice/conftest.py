import logging
logging.basicConfig(level=logging.DEBUG)

import pytest
import tornado.ioloop
import tornado.web

import multiprocessing



class ApplicationServerProcess(multiprocessing.Process):
    """A process-based Simple application server in testing. """

    def __init__(self):
        self.application = None
        self.host = None
        self.port = None
        self.home_url = None
        super(ApplicationServerProcess, self).__init__()

    def start(self, application, host='localhost', port=None):
        if self.application is not None:
            self.stop() # There is an running application, shut it down

        self.application = application

        self.host = host
        if port is None:
            port = self._get_unused_port()
        self.port = port
        self.home_url = 'http://%s:%d/' % (self.host, self.port)

        self.is_starting_done = multiprocessing.Event()
        super(ApplicationServerProcess, self).start()
        self.is_starting_done.wait()

        return self.home_url

    def run(self):
        self.ioloop = tornado.ioloop.IOLoop.instance()

        def handler(signum, frame):
            self.ioloop.add_callback(self.ioloop.stop)

        import signal
        signal.signal(signal.SIGTERM, handler)

        def ready():
            self.is_starting_done.set()
            raise ValueError('yer')

        self.ioloop.add_callback(ready)

        try:
            from tornado.httpserver import HTTPServer
            http_server = HTTPServer(self.application)
            http_server.listen(self.port, address=self.host)
            logger = logging.getLogger('tornado.general')
            logger.info('Application Server is Ready: ' +  self.home_url)
        finally:
            self.is_starting_done.set()

        self.ioloop.start()

    def stop(self):
        self.terminate()
        self.join()
        logger = logging.getLogger('tornado.general')
        logger.info('Server (%s) was down' % self.home_url)
        self.application = None

    # @staticmethod
    def _get_unused_port(self):
        import socket
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('localhost', 0))
            addr, port = s.getsockname()
            return port

# @pytest.fixture(scope="function")
# def application(request):
#     return None


@pytest.fixture(scope="function")
def app_url(request, application):

    server = ApplicationServerProcess()
    server.start(application)

    def finalizer():
        server.stop()
    request.addfinalizer(finalizer)


    return server.home_url
