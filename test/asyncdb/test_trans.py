# -*- coding: utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


import pytest
from domainics.asyncdb.sqlblock import set_dsn, transaction
from domainics.domobj import dobject, datt, dset

from domainics.sqltext import SQL


def setup_module(module):
    set_dsn(dsn='db2', url="postgresql://postgres@localhost/test")


@transaction.db
async def func1(db):
    await db.execute("SELECT 10 as sn")

    await db.execute("SELECT 11 as sn")

    await func2()

    await db.execute("SELECT 12 as sn")

@transaction.db
async def func2(db):
    await db.execute("SELECT 21 as sn")

    await func3()

@transaction.db2
async def func3(db2):
    await db2.execute("SELECT 31 as sn")

@pytest.mark.asyncio
async def test_trans():
    await func1()
