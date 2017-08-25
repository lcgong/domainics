# -*- coding: utf-8 -*-

import logging
logging.basicConfig(format='%(asctime)s %(message)s', level=logging.DEBUG)


import pytest
from domainics.asyncdb.sqlblock import set_dsn, transaction
from domainics.domobj import dobject, datt, dset
from domainics.sqltext import SQL


class Case:
    @transaction.db
    async def get_data(self, db):
        db << "SELECT 1 as sn, 'a' as name"
        await db
        return list(db)

@pytest.mark.asyncio
async def test_method(event_loop):

    c = Case()
    data = await c.get_data()
    assert len(data) == 1
    assert data[0].sn == 1
