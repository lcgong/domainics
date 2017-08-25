from domainics.domobj import dobject, datt
from domainics.domobj.dset import dset

from domainics.domobj.typing import DSet, DObject, AnyDObject

class t_b(dobject):
    a = datt(int)
    b = datt(int)
    c = datt(int)
    d = datt(int)

    __dobject_key__ = [a, b]


def test_dset_key_error():
    ASet = dset(t_b, _key=dict(a=datt(int))) # define a key of dset
    print()

    ds1 = ASet(a=1)
    ds1._add(t_b(b=12, c=13, d= 14))
    ds1._add(t_b(b=22, c=23, d=24))

    print('-------------------')

    ds2 = ASet(a=2)
    ds2._add(t_b(b=32, c=33, d= 34))
    ds2._add(t_b(b=42, c=43, d=44))
    assert ds2[0].a == 2
    print(ds1)
    print(ds2)
