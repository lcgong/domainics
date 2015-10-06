# -*- coding: utf-8 -*-

"""

Rehape dobject type:

    Select the required attributes:

        A._re('a', 'b', ...)
        A._re(a=True)

    # Add or replace attribute declaration

        A._re(c=datt(...), ...)

    Ignore some attribute

        A._re(e=False, _ignore='a')
        A._re(e=False, _ignore=['a', 'b'])

    Change the attributes of primary key:

        A._re(_pkey=['a', A.d])

    Change the bases

        A._re(_base=X)
        A._re(_base=[X, Y])

    Combine attributes from some dobjects
        A._re(_combine=X)
        A._re(_combine=[X, Y])

    Substitute attribute:

        A._re(..., _subst=dict(orgin_attr_name_attr1=new_name, ...))


    Change the type name:

        A._re(..., _name="new_type_name", ...)

Reshape dobject object:

    Shape or reshape a object in a new type:
        A()
        A(B(), ...)
        A(mapping_object, ...)
        A(some_object, ...)

        obj._re(A, ...)  # TBD: this form is still disputable.

    Set value of attribute
        obj._re(attr1 = val1, attr2 = val2, ...)


Attribute assigment:
    In face, the assigment of attribute is not object reshaping. The object is
    not reshape into a new object. Also, the assigment cannot change the value
    of the attributes as primary key.

    obj.attr1 = val1


Truth value of dobject - Whether it is not empty.

    Importantly, the python None value does not have semantic meaning in
    domain logics. Thus, we introduce the empty dobject concept.

    bool(obj)  check whether the dobject is empty.

    The following conditions are considered false:
    * No attribues defined in this dobject
    * All values of every dobject attribues are false. The following attribue
      values are considered false:
        * It's None.
        * It's equals tp the default value.
        * The truth value is false if the default is not specified.

Equivalence of dobject:

    When the primary key attribute is specified, this dobject is equal to
    the other if the attribues of primary key are equaled. Otherwise, all
    attributes are needed to be equaled if the two dobject are equaled.

    a == b

DSet
    A dset of dobject is a special dobject. It has also key attributes,
    but not non-key attributes. The dset as a group of dobject in domain should
    be better to be identified by some canonical infomation that is represented
    by key attributes. To be clear, although the item has a key, we assume that
    the key of item is enough to be identified in the dset domain. In bigger
    domain, the key of item would be invalid.

    When the dset object lives in a dobject, which this dset object is defined
    by a attribute or property in *dominion* dobject. The key attributes are
    needed to be attributes of dominion dobject. If the dominated dset is not
    set the key, the key of dominion object is used as the key of dset.

    The link between dominion object and item object ...

    A(sn) -> [ B(a_sn, ln) ] (_key=sn)


    dominion

    # class A(dobject):
    #     items = datt(dset(B, _key=sn, sn=B.sn), doc = "...")


    The key of item is required.

    __dominion_object__
    __dominion_class__
    __dobject_key__

    ASet = dset(A, _key=sn, _dominion=P, sn=A.sn)._re(_subst=dict(sn='ssn'))
    # sn is substituted to ssn, where the link is changed to ssn=A.sn
    s1 = ASet([adsds], sn=123)

    BSet = dset(B, _key=no, sn=A.sn) # make a new type dset
    s2 = BSet(s1, attr=val) # create a dset object

    # 





Specicial attribues:

    __dobject_key__
    __dobject_key_class__
    __dobject_att__

"""

#---------------------------------------------------------------------

"""





Domain:
A sphere of knowledge (ontology), influence, or activity. The subject area
to which the user applies a program is the domain of the software.

Entity:
An object that is not defined by its attributes, but rather by a thread of
continuity and its identity.

Value Object:
An object that contains attributes but has no conceptual identity.
They should be treated as immutable.

Aggregate:
A collection of objects that are bound together by a root entity,
otherwise known as an aggregate root.
The aggregate root guarantees the consistency of changes being made within the aggregate
by forbidding external objects from holding references to its members.

"""

#
from .typing import DSet, DObject
from .dattr import datt
from .dobject import dobject
from .dset import dset, DSetBase
