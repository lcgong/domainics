# -*- coding: utf-8 -*-

import logging
import tornado.ioloop
import tornado.httpserver
import tornado.process
import signal
import sys
import re
from collections import Mapping

def run_forever(application, port=None, num_processes=1) :
    """run server
    port='8888'
    port='8801, 8802, 8803'
    """

    logger = logging.getLogger('appserver')

    if sys.platform == 'win32' :
        title = 'Tornado'
        from .win32 import set_windows_console_title, set_windows_console_size
        set_windows_console_title(title)
        set_windows_console_size(120, 24)

    ports = []
    if port:
        if isinstance(port, str):
            ports.extends(re.split('[,;\s]+', port))
        elif isinstance(port, Mapping):
            for p in port:
                ports.append(p)

    if not ports:
        ports.append(8888)

    application.setup()

    if num_processes <= 0:
        import multiprocessing
        num_processes = multiprocessing.cpu_count()

    if application.settings.get('debug', False) and num_processes != 1:
        num_processes = 1
        logger.warn('In debug mode, it should be in single process mode')

    httpserver = tornado.httpserver.HTTPServer(application)
    for port in ports:
        httpserver.bind(port)
    httpserver.start(num_processes)

    ioloop = tornado.ioloop.IOLoop.instance()
    def sigint_int(s, f):
        logger.info('server is shutting')
        ioloop.add_callback(ioloop.stop)

    signal.signal(signal.SIGINT, sigint_int)

    print('Server Started. Press <CRTL-C> to kill server')
    ioloop.start() # 启动服务器
    logger.info('server stopped')
