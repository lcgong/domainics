
import pytest
from domainics.pillar import History

def test_generator_1():
    H = History()

    @H.confine
    def enum():
        for i in range(H.get('num')):
            yield i

    @H.confine
    def f1():
        H.let(num = 5)
        assert 10 == sum(i for i in enum())

    f1()
    assert len(H._frames) == 0 # no confined context left


def test_generator_2():

    H = History()

    @H.confine
    def enum():
        for i in range(H.get('num')):
            yield i
            if i == 3:
                raise ValueError(999)

    @H.confine
    def f1():
        H.let(num = 5)

        total = 0
        try:
            for i in enum():
                total += i
                print(i)
        except ValueError as exc:
            pass

        assert 6 == total

    f1()
    assert len(H._frames) == 0


def test_generator_2():

    H = History()

    @H.confine
    def enum():
        for i in range(H.get('num')):
            yield i
            if i == 3:
                raise ValueError(999)

    @H.confine
    def f1():
        H.let(num = 5)

        g = enum()

        total = 0
        total += next(g)
        total += next(g)
        total += next(g)
        total += next(g)
        g.close()

        assert 6 == total

    f1()
    assert len(H._frames) == 0
