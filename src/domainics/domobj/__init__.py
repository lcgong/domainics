# -*- coding: utf-8 -*-


"""



    dobject object

        :__primary_key__: It is a object of PrimaryKey that is a compound
                          of attribute value as a primary key of the domain object.



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
from .metaclass import datt, daggregate
from ._dobject import dobject
from .dset import dset
from ._reshape import reshape
