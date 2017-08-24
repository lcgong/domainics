# -*- coding: utf-8 -*-

import pytest

from datetime import datetime, date
from decimal import Decimal

import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)

import inspect
from domainics.sqltext import SQL
from domainics.asyncdb.sqlblock import set_dsn, transaction
from domainics.domobj import dobject, datt, dset
from domainics.asyncdb.dtable import dtable, dsequence, array, json_object
from domainics.asyncdb.schema import DBSchema


set_dsn(dsn='dba', url="postgresql://postgres@localhost/test")

async def setup_module(module):

    schema = DBSchema()
    schema.add_module(module)
    await schema.drop()
    await schema.create()


class t_a(dtable):
    a = datt(int)
    b = datt(array(str, dimensions=1))

    __dobject_key__ = [a]


@transaction.db(dsn='dba')
def test_array(db):

    ASet = dset(t_a)
    s1 = ASet()

    r1 = t_a(a=1)
    s1 += [r1]
    s1[0].b = ['a', 'b']
    assert s1[0].b == ['a', 'b']

    dmerge(s1)

    db << "SELECT * FROM t_a ORDER BY a"
    s2 = ASet(dbc)
    assert s2[0].b == ['a', 'b']

    print(s2)
