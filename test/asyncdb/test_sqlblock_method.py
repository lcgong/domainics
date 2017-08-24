# -*- coding: utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


import pytest
from domainics.asyncdb.sqlblock import set_dsn, transaction
from domainics.domobj import dobject, datt, dset
from domainics.sqltext import SQL


def setup_module(module):
    set_dsn(dsn='testdb', url="postgresql://postgres@localhost/test")

class Case:
    @transaction.db(dsn='testdb')
    async def get_data(self, db):
        db << "SELECT 1 as sn, 'a' as name"
        await db.execute()
        return list(db)

@pytest.fixture
async def test_method(event_loop):

    c = Case()
    data = await c.get_data()
    assert len(data) == 1
    assert data[0].sn == 1
