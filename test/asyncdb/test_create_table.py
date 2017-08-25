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


# from domainics.asyncdb import , , , , dmerge, drecall
#
# def setup_module(module):
#     set_dsn(dsn='dba', url="postgresql://postgres@localhost/test")
#

class t_a(dtable):
    a = datt(int)
    b = datt(array(str, dimensions=1))

    __dobject_key__ = [a]

@pytest.mark.asyncio
@transaction.db
async def test_array(db, setup_dsn):

    module = inspect.getmodule(t_a)

    schema = DBSchema()
    schema.add_module(module)
    await schema.drop()
    await schema.create()
