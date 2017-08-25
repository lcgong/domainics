
import pytest

import logging
logging.basicConfig(format='[%(asctime)s %(levelname)s] %(message)s', datefmt="%M:%S", level=logging.DEBUG)

from sqlblock.asyncpg import transaction
from sqlblock import SQL

@pytest.fixture(scope='session')
def setup_dsn():
    from sqlblock.asyncpg import set_dsn
    set_dsn(dsn='db', url="postgresql://postgres@localhost/test")


@pytest.fixture(scope='session')
def event_loop(request, setup_dsn):
    """
    To avoid the error that a pending task is attached to a different loop,
    create an instance of the default event loop for each test case.
    """
    import asyncio
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope='function')
@transaction.db
async def module_dtables(db, event_loop, request):

    from domainics.asyncdb.schema import DBSchema

    schema = DBSchema()
    schema.add_module(request.module)
    await schema.drop()
    await schema.create()
    print()

    def _cleanup():
        @transaction.db
        async def drop_table(db):
            await schema.drop()
            pass

        event_loop.run_until_complete(drop_table())
    request.addfinalizer(_cleanup)
