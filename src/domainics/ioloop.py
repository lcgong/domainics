# -*- coding: utf-8 -*-

import ctypes

def run(title=None, logging=True) :
    """服务主调度"""

    if title is None:
        title = 'Tornado'

    import tornado.ioloop
    server = tornado.ioloop.IOLoop.instance() 
    
    # 登记对CTRL-C消息的服务退出处理 
    import signal
    signal.signal(signal.SIGINT, lambda s, f: server.stop())

    import sys
    if sys.platform == 'win32' :
        # 设置Windows终端的标题和大小
        _set_windows_console_title(title)
        _set_windows_console_size(120, 24)


    print('Server Started. Press <CRTL-C> to kill server')
    server.start() # 启动服务器 
    print('Server stopped') 

def _set_windows_console_title(title):
    """设置Windows终端窗口标题"""

    k32 = ctypes.windll.kernel32 #调用Win内核API
    k32.SetConsoleTitleW(ctypes.c_wchar_p(title))
    
def _set_windows_console_size(width, height, line_buffer_size=1500) :
    """设置Windows终端窗口大小"""

    k32 = ctypes.windll.kernel32 #调用Win内核API
        
    STDOUT = -12        
    hndl = k32.GetStdHandle(STDOUT)

    bufsize = ctypes.wintypes._COORD(width, line_buffer_size)
    k32.SetConsoleScreenBufferSize(hndl, bufsize)

    # RECT(left, top, right, bottom)
    rect = ctypes.wintypes._SMALL_RECT(0, 0, width - 1, height) 
    k32.SetConsoleWindowInfo(hndl, ctypes.c_bool(True), ctypes.byref(rect))
    
