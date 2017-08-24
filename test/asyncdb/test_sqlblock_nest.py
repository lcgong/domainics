# -*- coding: utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


import pytest
from domainics.asyncdb.sqlblock import set_dsn, transaction
from domainics.domobj import dobject, datt, dset

from domainics.sqltext import SQL


def setup_module(module):
    set_dsn(dsn='db_a', url="postgresql://postgres@localhost/test")
    set_dsn(dsn='db_b', url="postgresql://postgres@localhost/test")

@transaction.db_b
@transaction.db_a
async def func1(db_a, db_b):

    await sub_func1(db_a, _dsn_db='db_a')
    await sub_func1(db_b, _dsn_db='db_b')

    await sub_func1(db_a, _dsn_db=db_a)
    await sub_func1(db_b, _dsn_db=db_b)

    with pytest.raises(ValueError) as exc:
        await sub_func1(db_b, _dsn_db='other_db')


@transaction._dsn_db
async def sub_func1(parent_db, _dsn_db=None):
    assert _dsn_db is not None
    assert _dsn_db is not parent_db
    assert parent_db is _dsn_db._parent_sqlblk

    assert _dsn_db._conn is _dsn_db._parent_sqlblk._conn

async def test_trans():
    await func1()
