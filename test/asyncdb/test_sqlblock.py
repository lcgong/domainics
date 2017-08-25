# -*- coding: utf-8 -*-

import pytest
from domainics.asyncdb.sqlblock import set_dsn, transaction
from domainics.domobj import dobject, datt, dset

from domainics.sqltext import SQL

# def setup_module(module):
#     set_dsn(dsn='testdb', url="postgresql://postgres@localhost/test")

@pytest.mark.asyncio
@transaction.db
async def test_func1(db, event_loop):
    db << """\
    SELECT 1 as name
    """
    async for r in db:
        print(r)

    db << """\
    DROP TABLE IF EXISTS test_123
    """
    await db.execute()

    #
    db << """\
    CREATE TABLE IF NOT EXISTS test_123 (
        sn INTEGER,
        name TEXT
    )
    """
    await db.execute()

    data = [(1, 'a'), (2, 'b'), (3, 'c')]
    table_name = "test_123"
    for sn, name in data:
        db << f"INSERT INTO {table_name} (sn, name) VALUES ({{sn}}, {{name}})"
        # dbconn << f""
        await db.execute()

    data = [{"sn":4, "name":'d'}, {"sn":5, "name":'e'}, {"sn":6, "name":'f'}]
    db << "INSERT INTO test_123 (sn, name) VALUES ({sn}, {name})"
    await db.executemany(data)

    db << SQL('SELECT sn, name FROM test_123;')
    async for r in db:
        print(r)

    db << SQL('SELECT sn, name FROM test_123')
    await db.execute()
    for r in db:
        print(r)

    class B(dobject):
        sn   = datt(int)
        name = datt(str)
        __dobject_key__ = [sn]

    db << SQL('SELECT sn, name FROM test_123')
    await db.execute()

    BSet = dset(B)
    bset = BSet(db)
    print(bset)
