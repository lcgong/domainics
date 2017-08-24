# -*- coding: utf-8 -*-

import sys

import inspect

from inspect import iscoroutinefunction, isgeneratorfunction, isfunction

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

    def pop(self, frame, name):
        del self._frames[frame][name]
        if not self._frames[frame] :
            del self._frames[frame]

    def get(self, name, frame=None):
        """ define name on pillar at a frame"""
        if frame is None:
            frame = sys._getframe(1)

        value = self.top(frame, name)

        return value

    def let(self, **kwargs):
        """ define name on pillar at a frame"""
        frame = kwargs.pop('_frame', None)
        if frame is None:
            frame = sys._getframe(1)

        ctx = self.current_context(frame)
        if ctx is None:
            raise NoConfineError('no confined function: ' + frame.f_code.co_name)

        for k, v in kwargs.items():
            ctx[k] = v

    def clear_context(self, frame):
        """ clear a frame on pillar """
        if frame in self._frames :
            del self._frames[frame]


    def has_name(self, name, frame=None) :
        """find the top object at frame stack"""
        if frame is None:
            frame = sys._getframe(1)

        while frame:
            objs = self._frames.get(frame)
            if objs is not None and name in objs:
                return True

            if frame == self._start_frame:
                break

            frame = frame.f_back

        return False

    def top(self, frame, name) :
        """find the top object at frame stack"""
        assert name is not None

        while frame:
            objs = self._frames.get(frame)
            if objs is not None and name in objs:
                return objs[name]

            if frame == self._start_frame:
                break

            frame = frame.f_back

        raise NoBoundObjectError(name)


    def current_context(self, frame):
        while frame:
            context = self._frames.get(frame, None)
            if context is not None:
                return context

            if frame == self._start_frame:
                break

            frame = frame.f_back

        return None

    def printHist(self, frame=None) :
        if frame is None:
            frame = sys._getframe(1)

        while frame:
            objs = self._frames.get(frame, None)
            if objs:
                print(123, frame, objs)
            else:
                print(456, frame)

            if frame == self._start_frame:
                break

            frame = frame.f_back

    def confine(self, *args, exit_callback=None, **kwargs):
        """
        @confine
        def func() :
            pass

        @confine(a=1, b=2, exit_callback=..)
        def funct():
            pass

        co_func = confine(func, a=1, b=2, exit_callback=exit_func)
        """

        if len(args) > 0:
            f = args[0]
            if iscoroutinefunction(f) or isfunction(f) or isgeneratorfunction(f):
                return self.bound(f, kwargs.items(), exit_callback=exit_callback)
        else:
            return lambda f : self.bound(f, kwargs.items(), exit_callback=exit_callback)

        # return self.bound(func, [], exit_callback=None)

    def bound(self, func, bindings, exit_callback=None):
        """ bound func with binding pillars.
        When the bounded has returned, the exit_callback will be called.

        :param func: the bouding function
        :param bindings: a list binding variables [(pillar, value)]
        :param exit_callback: a callback function with three arguments. exit_callback(exc_type, exc_val, tb)
        """

        if inspect.iscoroutinefunction(func):
            async def _bound_coroutine_func(*args, **kwargs):
                coroutine = func(*args, **kwargs)
                frame = coroutine.cr_frame

                if frame in self._frames:
                    errmsg = 'cannot reenter the same frame(%s)' % frame
                    raise PillarError(errmsg)

                assert frame not in self._frames
                self._frames[frame] = {}
                for k, v in bindings:
                    self.push(frame, k, v)

                ret = None
                try:
                    ret = await coroutine
                finally:
                    self.clear_context(frame)

                    if exit_callback:
                        exc_type, exc_val, tb = sys.exc_info()
                        if iscoroutinefunction(exit_callback):
                            await exit_callback(exc_type, exc_val, tb)
                        else:
                            exit_callback(exc_type, exc_val, tb)

                return ret

            return _bound_coroutine_func

        elif inspect.isgeneratorfunction(func):
            def _bound_func(*args, **kwargs):
                generator = func(*args, **kwargs)

                frame = generator.gi_frame
                if frame in self._frames:
                    errmsg = 'cannot reenter the same frame(%s)' % frame
                    raise PillarError(errmsg)

                assert frame not in self._frames
                self._frames[frame] = {}

                for pillar, obj in bindings:
                    self.push(frame, pillar, obj)

                def _closed_handler(exc_type, exc_val, tb):
                    if exit_callback:
                        if exc_type == StopIteration:
                            return

                        exit_callback(exc_type, exc_val, tb)

                    self.clear_context(frame)

                proxied = GeneratorClosedProxy(generator, _closed_handler)
                # proxied = setup_closed_state_func(generator, _closed_handler)
                return proxied

            return _bound_func

        elif inspect.isfunction(func):
            def _bound_func(*args, **kwargs):
                frame = sys._getframe(1)

                if frame in self._frames:
                    # For async generator, this oringal func is called again
                    # in the same frame of coroutine.
                    return func(*args, **kwargs)

                assert frame not in self._frames
                self._frames[frame] = {}
                for k, obj in bindings:
                    self.push(frame, k, obj)

                try:
                    return func(*args, **kwargs)
                finally:
                    self.clear_context(frame)
                    if exit_callback:
                        exc_type, exc_val, tb = sys.exc_info()
                        exit_callback(exc_type, exc_val, tb)


            return _bound_func
        else:
            raise TypeError("Not Implemented")


class PillarError(Exception):
    pass

class NoBoundObjectError(PillarError):
    pass

class NoConfineError(PillarError):
    pass


class Pillars:
    __slots__ = ('__pillar__')

    def __init__(self, history):
        self.__pillar__ = history

    def __getattr__(self, name):
        getter = self.__pillar__.get
        return getter(name, frame=sys._getframe(1))


def pillar_class(theclass, excludes=None):
    """make pillar class like theclass"""

    if excludes is None:
        excludes = []


    _special_func_names = [
        '__abs__', '__add__', '__and__', '__call__', '__cmp__', '__coerce__',
        '__contains__', '__delitem__', '__delslice__', '__div__', '__divmod__',
        '__eq__', '__float__', '__floordiv__', '__ge__', '__getitem__',
        '__getslice__', '__gt__', '__hash__', '__hex__',
        '__int__', '__invert__',  '__iter__', '__le__', '__len__',
        '__long__', '__lshift__', '__lt__', '__mod__', '__mul__', '__ne__',
        '__neg__', '__oct__', '__or__', '__pos__', '__pow__', '__radd__',
        '__rand__', '__rdiv__', '__rdivmod__', '__reduce__', '__reduce_ex__',
        '__repr__', '__reversed__', '__rfloorfiv__', '__rlshift__', '__rmod__',
        '__rmul__', '__ror__', '__rpow__', '__rrshift__', '__rshift__', '__rsub__',
        '__rtruediv__', '__rxor__', '__setitem__', '__setslice__', '__sub__',
        '__truediv__', '__xor__', '__next__',]

    _special_ioptr_names = [
        '__iadd__', '__iand__', '__idiv__', '__idivmod__', '__ifloordiv__',
        '__ilshift__', '__imod__', '__imul__', '__ior__', '__ipow__', '__irshift__',
        '__isub__', '__itruediv__', '__ixor__',
    ]

    def _get_this_object(pillar, frame):
        hist  = object.__getattribute__(pillar, "_pillar_history")
        return hist.top(frame, id(pillar))


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

    def __init__(self, _history):
        object.__setattr__(self, "_pillar_history", _history)

    def _this_object(self):
        return _get_this_object(self, sys._getframe(1))


    def __getattribute__(self, name):
        if name == '_this_object' or name.startswith('_pillar_'):
            return object.__getattribute__(self, name)

        return getattr(_get_this_object(self, sys._getframe(1)), name)

    def __setattr__(self, name, value):
        if name == '_this_object' or name.startswith('_pillar_'):
            raise Pillar('Attribute ' + name + ' read-only')

        setattr(_get_this_object(self, sys._getframe(1)), name, value)

    def __repr__(self):
        cls   = object.__getattribute__(self, "__class__")
        obj   = _get_this_object(self, sys._getframe(1))
        return '%s(id=%r, this_object=%r)' % (cls.__name__, id(self), obj)

    attrs = dict(
        __slots__       = ('_pillar_history',),
        __init__        = __init__,
        _this_object    = property(_this_object),
        __getattribute__= __getattribute__,
        __setattr__     = __setattr__,
        __repr__        = __repr__
        )

    needed = []
    for name in _special_func_names:
        if name not in excludes and hasattr(theclass, name) and name not in attrs:
            needed.append((name, _make_func_proxy(name)))

    for name in _special_ioptr_names:
        if name not in excludes and hasattr(theclass, name) and name not in attrs:
            needed.append((name, _make_ioptr_proxy(name)))

    attrs.update(needed)

    return type('Pillar', (object,), attrs)



class GeneratorClosedProxy:
    __slots__ = ('__gen_obj__', '__closed_callback__')

    def __init__(self, genobj, closed_callback):
        self.__gen_obj__ = genobj
        self.__closed_callback__ = closed_callback

    def __getattribute__(self, name):
        return getattr(object.__getattribute__(self, "__gen_obj__"), name)

    def __next__(self):
        """ """
        genobj = object.__getattribute__(self, "__gen_obj__")
        try:
            return genobj.__next__()
        except :
            if inspect.getgeneratorstate(genobj) == 'GEN_CLOSED':
                etype, eval, tb = sys.exc_info()
                object.__getattribute__(self, "__closed_callback__")(etype, eval, tb)

            raise

    def __del__(self):
        obj = object.__getattribute__(self, "__gen_obj__")
        if hasattr(obj, '__del__'):
            obj.__del__()

        etype, eval, tb = sys.exc_info()
        object.__getattribute__(self, "__closed_callback__")(etype, eval, tb)

    def __iter__(self) :
        obj = object.__getattribute__(self, "__gen_obj__")
        val = obj.__getattribute__('__iter__')()
        assert obj == val
        return self


P = Pillars(History())

_pillar_history = History()
