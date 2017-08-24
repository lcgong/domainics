

import aiohttp
from domainics.asyncdb.pgsql import set_dsn


async def test1(event_loop):
    await set_dsn(name="DEFAULT", url="postgresql://postgres@localhost/test34")
