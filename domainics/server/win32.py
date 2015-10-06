# -*- coding: utf-8 -*-

import ctypes


def set_windows_console_title(title):

    k32 = ctypes.windll.kernel32 #调用Win内核API
    k32.SetConsoleTitleW(ctypes.c_wchar_p(title))

def set_windows_console_size(width, height, line_buffer_size=1500) :

    k32 = ctypes.windll.kernel32

    STDOUT = -12
    hndl = k32.GetStdHandle(STDOUT)

    bufsize = ctypes.wintypes._COORD(width, line_buffer_size)
    k32.SetConsoleScreenBufferSize(hndl, bufsize)

    # RECT(left, top, right, bottom)
    rect = ctypes.wintypes._SMALL_RECT(0, 0, width - 1, height)
    k32.SetConsoleWindowInfo(hndl, ctypes.c_bool(True), ctypes.byref(rect))
