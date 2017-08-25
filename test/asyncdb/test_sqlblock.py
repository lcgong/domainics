# -*- coding: utf-8 -*-

import pytest
from domainics.domobj import dobject, datt, dset

from sqlblock.asyncpg import transaction
from sqlblock import SQL

@pytest.mark.asyncio
@transaction.db
async def test_func_rshift(db, event_loop):
    SQL("""
    SELECT 1 as sn, 'tom' as name
    """) >> db
    await db
    for r in db: print(r)

    SQL("""
    SELECT 1 as sn, 'tom' as name
    """) >> db
    for r in await db: print(r)

    SQL("""
    SELECT 1 as sn, 'tom' as name
    """) >> db
    async for r in db: print(r)

    sn = 10
    SQL("""
    SELECT {sn}::integer as sn, 'tom' as name
    """) >> db
    await db

    SQL("""
    SELECT {sn}::integer as sn, 'tom' as name
    """) >> db
    await db(sn=20)


    SQL("""
    SELECT {sn}::integer as sn, 'tom' as name
    """) >> db
    await db([dict(sn=200), dict(sn=201)])

@pytest.mark.asyncio
@transaction.db
async def test_func_rshift_exc1(db, event_loop):
    SQL("""
    SELECT 1 as sn, 'tom' as name
    """) >> db
    # await db
    with pytest.raises(ValueError): # need to await db
        for r in db:
            print(r)

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
    await db

    #
    db << """\
    CREATE TABLE IF NOT EXISTS test_123 (
        sn INTEGER,
        name TEXT
    )
    """
    await db

    data = [(1, 'a'), (2, 'b'), (3, 'c')]
    table_name = "test_123"
    for sn, name in data:
        db << f"INSERT INTO {table_name} (sn, name) VALUES ({{sn}}, {{name}})"
        # dbconn << f""
        await db

    data = [{"sn":4, "name":'d'}, {"sn":5, "name":'e'}, {"sn":6, "name":'f'}]
    db << "INSERT INTO test_123 (sn, name) VALUES ({sn}, {name})"
    await db(data)

    db << SQL('SELECT sn, name FROM test_123;')
    async for r in db:
        print(r)

    db << SQL('SELECT sn, name FROM test_123')
    await db
    for r in db:
        print(r)

    class B(dobject):
        sn   = datt(int)
        name = datt(str)
        __dobject_key__ = [sn]

    db << SQL('SELECT sn, name FROM test_123')
    await db

    BSet = dset(B)
    bset = BSet(db)
    print(bset)
