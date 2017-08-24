# -*- coding: utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


import pytest
from domainics.asyncdb.sqlblock import set_dsn, transaction
from domainics.domobj import dobject, datt, dset

from domainics.sqltext import SQL


def setup_module(module):
    set_dsn(dsn='testdb', url="postgresql://postgres@localhost/test")


@transaction.db(dsn='testdb')
async def func1(db):
    db << "SELECT 10 as sn"
    await db.execute()

    db << "SELECT 11 as sn"
    await db.execute()

    await func2()

    db << "SELECT 12 as sn"
    await db.execute()

import sys
@transaction.db1(dsn='testdb')
async def func2(db1):

    db1 << "SELECT 21 as sn"
    await db1.execute()

@transaction.db(dsn='testdb')
async def func3(db):

    db << "SELECT 31 as sn"
    await db.execute()

async def test_trans():
    await func1()
