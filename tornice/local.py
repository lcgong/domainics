# -*- coding: utf-8 -*-

import sys

class LocalStack:
    
    _frame_locals = {}

    def __getattr__(self, name):

        frame = sys._getframe(1) 
        while frame:
            env = self._frame_locals.get(frame)
            if env is not None:
                if name in env:
                    return env[name]

            frame = frame.f_back
        raise AttributeError('no attribute: ' + name)

class local_env:

    def __init__(self, **settings):
        self.settings = settings

    def __enter__(self):
        current_frame = sys._getframe(1)
        
        frame_locals = S._frame_locals
        if current_frame not in frame_locals:
            frame_locals[current_frame] = {}
        
        envs = frame_locals[current_frame]
        for name, value in self.settings.items():
            envs[name] = value        

        self.current_frame = current_frame
        return None 

    def __exit__ (self, etyp, ev, tb):
        current_frame = sys._getframe(1)
        del S._frame_locals[current_frame]

        return False    

S = LocalStack()
