# -*- coding: utf-8 -*-

import pytest
from domainics.domobj import dobject, datt, dset

from sqlblock.asyncpg import transaction
from sqlblock import SQL

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
