# -*- coding: utf-8 -*-

import sys

import inspect

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
        """find the top object at frame stack"""
        assert name is not None

        while frame:
            objs = self._frames.get(frame)
            if objs is not None and name in objs:
                return objs[name]

            if frame == self._start_frame:
                break

            frame = frame.f_back

        return None

    def bound(self, func, bindings, exit_callback=None):
        """ bound func with binding pillars.
        When the bounded has returned, the exit_callback will be called.

        :param func: the bouding function
        :param bindings: a list binding variables [(pillar, value)]
        :param exit_callback: a callback function with three arguments. exit_callback(exc_type, exc_val, tb)
        """

        def bound_func(*args, **kwargs):
            frame = sys._getframe(1)

            if self.has_frame(frame):
                errmsg = 'cannot reenter the same frame(%s)' % frame
                raise PillarError(errmsg)

            for pillar, obj in bindings:
                self.push(frame, id(pillar), obj)

            ret = None
            try:
                ret = func(*args, **kwargs)
            finally:
                for pillar, _ in bindings:
                    self.pop(frame, id(pillar))
                if inspect.isgenerator(ret):
                    ret = self._confine_gen(bindings, ret, exit_callback)
                else:
                    if exit_callback:
                        exc_type, exc_val, tb = sys.exc_info()
                        exit_callback(exc_type, exc_val, tb)

            return ret

        return bound_func


    def _confine_gen(self, bindings, generator, closed_callback=None):
        # generator = genfunc()
        frame = generator.gi_frame
        for pillar, obj in bindings:
            self.push(frame, id(pillar), obj)

        def _closed_handler(exc_type, exc_val, tb):
            if closed_callback:
                if exc_type == StopIteration:
                    return

                closed_callback(exc_type, exc_val, tb)

            nonlocal frame
            for pillar, _ in bindings:
                self.pop(frame, id(pillar))

        proxied = GeneratorClosedProxy(generator, _closed_handler)
        def wrapped_genfunc():
            nonlocal proxied
            yield from proxied

        return proxied

class PillarError(Exception):
    pass

class NoBoundObjectError(PillarError):
    pass

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
    __slots__ = ('_gen_obj', '_closed_callback')

    def __init__(self, genobj, closed_callback):
        """ """
        object.__setattr__(self, "_gen_obj", genobj)
        object.__setattr__(self, "_closed_callback", closed_callback)

        print(self, genobj)

    def __getattribute__(self, name):
        print(name)
        return getattr(object.__getattribute__(self, "_gen_obj"), name)

    def __next__(self):
        """ """
        genobj = object.__getattribute__(self, "_gen_obj")
        try:
            return genobj.__getattribute__('__next__')()
        except :

            if inspect.getgeneratorstate(genobj) == 'GEN_CLOSED':
                etype, eval, tb = sys.exc_info()
                object.__getattribute__(self, "_closed_callback")(etype, eval, tb)

            raise

    def __del__(self):
        obj = object.__getattribute__(self, "_gen_obj")
        if hasattr(obj, '__del__'):
            getattr(obj, '__del__')()

        etype, eval, tb = sys.exc_info()
        object.__getattribute__(self, "_closed_callback")(etype, eval, tb)


    def __delattr__(self, name):
        delattr(object.__getattribute__(self, "_gen_obj"), name)

    def __setattr__(self, name, value):
        setattr(object.__getattribute__(self, "_gen_obj"), name, value)

    def __iter__(self) :
        obj = object.__getattribute__(self, "_gen_obj")
        val = obj.__getattribute__('__iter__')()
        assert obj == val
        return self

    def __doc__(self) :
        obj = object.__getattribute__(self, "_gen_obj")
        return obj.__getattribute__('__doc__')

    def __repr__(self):
        obj = object.__getattribute__(self, "_gen_obj")
        return '<proxy %r at 0x%x>' % (obj, id(self))

    def __hash__(self):
        obj = object.__getattribute__(self, "_gen_obj")
        return obj.__getattribute__('__hash__')()

    def __format__(self, *args, **kwargs):
        obj = object.__getattribute__(self, "_gen_obj")
        return obj.__getattribute__('__format__')(*args, **kwargs)

    def __reduce__(self, *args, **kwargs):
        obj = object.__getattribute__(self, "_gen_obj")
        return obj.__getattribute__('__reduce__')(*args, **kwargs)

    def __reduce_ex__(self, *args, **kwargs):
        obj = object.__getattribute__(self, "_gen_obj")
        return obj.__getattribute__('__reduce_ex__')(*args, **kwargs)

    def __eq__(self, *args, **kwargs):
        obj = object.__getattribute__(self, "_gen_obj")
        return obj.__getattribute__('__eq__')(*args, **kwargs)

    def __ne__(self, *args, **kwargs):
        obj = object.__getattribute__(self, "_gen_obj")
        return obj.__getattribute__('__ne__')(*args, **kwargs)

    def __gt__(self, *args, **kwargs):
        obj = object.__getattribute__(self, "_gen_obj")
        return obj.__getattribute__('__gt__')(*args, **kwargs)

    def __ge__(self, *args, **kwargs):
        obj = object.__getattribute__(self, "_gen_obj")
        return obj.__getattribute__('__ge__')(*args, **kwargs)

    def __le__(self, *args, **kwargs):
        obj = object.__getattribute__(self, "_gen_obj")
        return obj.__getattribute__('__le__')(*args, **kwargs)

    def __lt__(self, *args, **kwargs):
        obj = object.__getattribute__(self, "_gen_obj")
        return obj.__getattribute__('__lt__')(*args, **kwargs)




_pillar_history = History()
