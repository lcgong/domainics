# -*- coding: utf-8 -*-

import sys

class History:
    '''object timeline history'''
    
    _frames = {}

    def __init__(self):
        self._start_frame = sys._getframe(1) 

    def __getattr__(self, name):

        frame = sys._getframe(1) 
        while frame:
            env = self._frames.get(frame)
            if env is not None:
                if name in env:
                    return env[name]

            frame = frame.f_back
        raise AttributeError('no attribute: ' + name)

    def has_frame(self, frame):
        return frame in self._frames 

    def push(self, frame, name, value):
        """If frame has been stacked, ignore it"""
        if frame not in self._frames:
            self._frames[frame] = {}

        envs = self._frames[frame]
        envs[name] = value
        # print('ENTER:', frame, self._frames[frame])


    def pop(self, frame, name):
        # print(' EXIT:', frame, self._frames[frame])
        del self._frames[frame][name]
        if not self._frames[frame] :
            del self._frames[frame]

    def top(self, frame, name) :
        assert name is not None

        while frame:
            env = self._frames.get(frame)
            if env is not None:
                if name in env:
                    return env[name]
            if frame == self._start_frame:
                break

            frame = frame.f_back
        return None

    def confine(self, *bindings):
        """ (lifeline, obj), (lifeline, obj) ]"""
        return _Confine(self, *bindings)

class LifelineError(Exception):
    pass

class NoBoundObjectError(LifelineError):
    pass

class _Confine:

    def __init__(self, stack, *bindings):
        self._stack = stack
        self._bindings = bindings

    def __enter__(self):
        frame = sys._getframe(1)
        if self._stack.has_frame(frame):
            errmsg = 'cannot reenter the same frame(%s)' % frame
            raise LifelineError(errmsg)

        print(4444, self._bindings)
        for lifeline, obj in self._bindings:
            self._stack.push(frame, id(lifeline), obj)

        return None 

    def __exit__ (self, etyp, ev, tb):
        frame = sys._getframe(1)
        for lifeline, _ in self._bindings:
            self._stack.pop(frame, id(lifeline))
        
        return False

def _make_func_proxy(name):
    def invoke(self, *args, **kw):
        obj = _get_this_object(self, sys._getframe(1))
        if obj is None:
            raise NoBoundObjectError('id=' + str(id(self)))
        return getattr(obj, name)(*args, **kw)
    return invoke

def _make_ioptr_proxy(name):
    def optr(self, *args, **kw):
        obj = _get_this_object(self, sys._getframe(1))
        if obj is None:
            raise NoBoundObjectError('id=' + str(id(self)))
        getattr(obj, name)(*args, **kw)
        return self
    return optr

def _get_this_object(lifeline, frame):
    hist  = object.__getattribute__(lifeline, "_lifeline_history")
    return hist.top(frame, id(lifeline))

class Lifeline(object):
    __slots__ = ("_lifeline_history")

    def __init__(self, _history):
        object.__setattr__(self, "_lifeline_history", _history)

    @property
    def _this_object(self):
        return _get_this_object(self, sys._getframe(1))


    def __getattribute__(self, name):
        if name == '_this_object' or name.startswith('_lifeline_'):
            return object.__getattribute__(self, name)

        return getattr(_get_this_object(self, sys._getframe(1)), name)

    def __setattr__(self, name, value):
        if name == '_this_object' or name.startswith('_lifeline_'):
            raise Lifeline('Attribute ' + name + ' read-only')

        setattr(_get_this_object(self, sys._getframe(1)), name, value)
    
    # def __hash__(self):
    #     object.__hash__(self)

    # def __str__(self):
    #     return object.__getattribute__(self, "__repr__")()
    
    def __repr__(self):
        cls   = object.__getattribute__(self, "__class__")
        obj   = _get_this_object(self, sys._getframe(1))
        return '%s(id=%r, this_object=%r)' % (cls.__name__, id(self), obj)

    # def __nonzero__(self):
    #     return bool(_get_this_object(self, sys._getframe(1)))

    def __delattr__(self, name):
        delattr(_get_this_object(self, sys._getframe(1)), name)

    __len__         = _make_func_proxy('__len__')

    __iter__        = _make_func_proxy('__iter__')
    __call__        = _make_func_proxy('__call__')

    __abs__         = _make_func_proxy('__abs__')
    __add__         = _make_func_proxy('__add__')
    __and__         = _make_func_proxy('__and__')
    
    __cmp__         = _make_func_proxy('__cmp__')
    __coerce__      = _make_func_proxy('__coerce__')
    __contains__    = _make_func_proxy('__contains__')
    __div__         = _make_func_proxy('__div__')
    __divmod__      = _make_func_proxy('__divmod__')
    

    
    __getitem__     = _make_func_proxy('__getitem__')
    __getslice__    = _make_func_proxy('__getslice__')
    __setitem__     = _make_func_proxy('__setitem__')
    __setslice__    = _make_func_proxy('__setslice__')
    __delitem__     = _make_func_proxy('__delitem__')
    __delslice__    = _make_func_proxy('__delslice__')

    __floordiv__    = _make_func_proxy('__floordiv__')
    __ge__          = _make_func_proxy('__ge__')
    __eq__          = _make_func_proxy('__eq__')
    __gt__          = _make_func_proxy('__gt__')
     
    __hex__         = _make_func_proxy('__hex__')
    __oct__         = _make_func_proxy('__oct__')
    __long__        = _make_func_proxy('__long__')
    __float__       = _make_func_proxy('__float__')
    
    __iadd__        = _make_ioptr_proxy('__iadd__')
    __iand__        = _make_ioptr_proxy('__iand__')
    __idiv__        = _make_ioptr_proxy('__idiv__')
    __idivmod__     = _make_ioptr_proxy('__idivmod__')
    __ifloordiv__   = _make_ioptr_proxy('__ifloordiv__')
    __ilshift__     = _make_ioptr_proxy('__ilshift__')
    __imod__        = _make_ioptr_proxy('__imod__')
    __imul__        = _make_ioptr_proxy('__imul__')
    __int__         = _make_ioptr_proxy('__int__')
    __invert__      = _make_ioptr_proxy('__invert__')
    __ior__         = _make_ioptr_proxy('__ior__')
    __ipow__        = _make_ioptr_proxy('__ipow__')
    __irshift__     = _make_ioptr_proxy('__irshift__')
    __isub__        = _make_ioptr_proxy('__isub__')
    __itruediv__    = _make_ioptr_proxy('__itruediv__')
    __ixor__        = _make_ioptr_proxy('__ixor__')





    __le__          = _make_func_proxy('__le__')
    __lshift__      = _make_func_proxy('__lshift__')
    __lt__          = _make_func_proxy('__lt__')
    __mod__         = _make_func_proxy('__mod__')
    __mul__         = _make_func_proxy('__mul__')
    __ne__          = _make_func_proxy('__ne__')
    __neg__         = _make_func_proxy('__neg__')
    __or__          = _make_func_proxy('__or__')

    __pos__         = _make_func_proxy('__pos__')
    __pow__         = _make_func_proxy('__pow__')
    __radd__        = _make_func_proxy('__radd__')
    __rand__        = _make_func_proxy('__rand__')
    __rdiv__        = _make_func_proxy('__rdiv__')
    __rdivmod__     = _make_func_proxy('__rdivmod__')
    __reduce__      = _make_func_proxy('__reduce__')
    __reduce_ex__   = _make_func_proxy('__reduce_ex__')

    __reversed__    = _make_func_proxy('__reversed__')
    __rfloorfiv__   = _make_func_proxy('__rfloorfiv__')
    __rlshift__     = _make_func_proxy('__rlshift__')
    

    __rmod__        = _make_func_proxy('__rmod__')
    __rmul__        = _make_func_proxy('__rmul__')
    __ror__         = _make_func_proxy('__ror__')
    __rpow__        = _make_func_proxy('__rpow__')
    __rrshift__     = _make_func_proxy('__rrshift__')
    __rshift__      = _make_func_proxy('__rshift__')
    __rsub__        = _make_func_proxy('__rsub__')
    __rtruediv__    = _make_func_proxy('__rtruediv__')
    __rxor__        = _make_func_proxy('__rxor__')
    __sub__         = _make_func_proxy('__sub__')
    __truediv__     = _make_func_proxy('__truediv__')
    __xor__         = _make_func_proxy('__xor__')


