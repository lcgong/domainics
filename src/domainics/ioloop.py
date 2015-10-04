# -*- coding: utf-8 -*-

import ctypes

def run(app, port=None) :
    """run server"""

    if title is None:
        title = 'Tornado'

    import sys
    if sys.platform == 'win32' :
        _set_windows_console_title(title)
        _set_windows_console_size(120, 24)

    import tornado.ioloop
    server = tornado.ioloop.IOLoop.instance()

    import signal
    signal.signal(signal.SIGINT, lambda s, f: server.stop())



    print('Server Started. Press <CRTL-C> to kill server')
    server.start() # 启动服务器
    print('Server stopped')
