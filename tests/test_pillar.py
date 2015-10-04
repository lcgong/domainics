
import pytest

from domainics.pillar import pillar_class, History
from domainics.pillar import PillarError, NoBoundObjectError


def test_1():
    hist = History()
    Pillar = pillar_class(str)
    a = Pillar(hist)

    def f1():
        nonlocal a
        print(201, a)
        hist.bound(f2, [(a, 'B')])()
        print(202, a)

    def f2():
        nonlocal a
        print(300, a)

    print(101, a)
    hist.bound(f2, [(a, 'A')])()
    print(102, a)

def test_2():
    hist = History()
    Pillar = pillar_class(str)
    a = Pillar(hist)

    def f1():
        nonlocal a
        print(201, a)
        g = hist.bound(f2, [(a, 'B')])()


        for r0, r1 in g:
            print('%s $ %d-%s' % (a._this_object, r0, r1))
            # print(g.close)
            # g.close()
            if r0 == 103:
                break

        print(202, a)

    def f2():
        nonlocal a
        for i in range(5):
            yield i + 100, str(a._this_object)
            # raise ValueError()

    print(101, a)
    hist.bound(f1, [(a, 'A')])()
    print(102, a)


def test_3():
    hist = History()
    StrPillar = pillar_class(str)
    a = StrPillar(hist)

    target_obj = None
    def f():
        nonlocal a, target_obj
        print(201, a)
        assert a == target_obj

    f1 = hist.bound(f, [(a, 'A1')])
    f2 = hist.bound(f, [(a, 'A2')])

    print(101, a)
    target_obj = 'A1'
    f1()
    print(102, a)


    print(101, a)
    target_obj = 'A2'
    f2()
    print(102, a)
