# -*- coding: utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


import pytest
from domainics.asyncdb.sqlblock import set_dsn, transaction
from domainics.domobj import dobject, datt, dset

from domainics.sqltext import SQL

# def setup_module(module):
set_dsn(dsn='testdb', url="postgresql://postgres@localhost/test")

@transaction.dbconn(dsn='testdb')
async def test_func1(dbconn, col):
    dbconn << """\
    SELECT 1 as name
    """
    async for r in dbconn:
        print(r)

    dbconn << """\
    DROP TABLE IF EXISTS test_123
    """
    await dbconn.execute()

    #
    dbconn << """\
    CREATE TABLE IF NOT EXISTS test_123 (
        sn INTEGER,
        name TEXT
    )
    """
    await dbconn.execute()

    #
    dbconn << """\
    INSERT INTO test_123 VALUES (1, 'a'), (2, 'b')
    """
    await dbconn.execute()

    dbconn << SQL('SELECT sn, name FROM test_123')
    async for r in dbconn:
        print(r)

    dbconn << SQL('SELECT sn, name FROM test_123')
    await dbconn.execute()
    for r in dbconn:
        print(r)

    class B(dobject):
        sn   = datt(int)
        name = datt(str)
        __dobject_key__ = [sn]

    dbconn << SQL('SELECT sn, name FROM test_123')
    await dbconn.execute()

    BSet = dset(B)
    bset = BSet(dbconn)
    print(bset)

import asyncio
loop = asyncio.get_event_loop()
loop.run_until_complete(test_func1(col=123))
loop.close()
