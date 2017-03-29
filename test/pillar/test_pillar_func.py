
import pytest

from domainics.pillar import pillar_class, History, Pillars
from domainics.pillar import PillarError, NoBoundObjectError



def test_pillar():
    P = Pillars(History())

    @P.__pillar__.confine
    def f1():
        P.__pillar__.let(a=101, b=102)
        assert 101 == P.a and 102 == P.b
        f2()
        assert 101 == P.a and 102 == P.b

    @P.__pillar__.confine
    def f2():
        assert 101 == P.a and 102 == P.b
        P.__pillar__.let(b=202)
        assert 101 == P.a and 202 == P.b

        with pytest.raises(NoBoundObjectError):
            P.sql

    f1()
