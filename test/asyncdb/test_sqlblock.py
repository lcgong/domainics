# -*- coding: utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


import pytest
from domainics.asyncdb.sqlblock import set_dsn, transaction
from domainics.domobj import dobject, datt, dset

from domainics.sqltext import SQL


def setup_module(module):
    set_dsn(dsn='testdb', url="postgresql://postgres@localhost/test")


@transaction.dbconn(dsn='testdb')
async def test_func1(dbconn, event_loop):
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

    data = [(1, 'a'), (2, 'b'), (3, 'c')]
    table_name = "test_123"
    for sn, name in data:
        dbconn << f"INSERT INTO {table_name} (sn, name) VALUES ({{sn}}, {{name}})"
        # dbconn << f""
        await dbconn.execute()

    data = [{"sn":4, "name":'d'}, {"sn":5, "name":'e'}, {"sn":6, "name":'f'}]
    dbconn << "INSERT INTO test_123 (sn, name) VALUES ({sn}, {name})"
    await dbconn.executemany(data)

    dbconn << SQL('SELECT sn, name FROM test_123;')
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
