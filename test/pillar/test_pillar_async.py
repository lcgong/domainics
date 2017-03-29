
import pytest
import asyncio
import sys
from domainics.pillar import History

@pytest.mark.asyncio
async def test_pillar_async_func():
    H = History()

    @H.confine
    async def f3() :
        assert 101 == H.get('a') and 202 == H.get('b')

        H.let(a = 301)
        # H.printHist()

        assert 301 == H.get('a')
        assert 202 == H.get('b')

        return 250

    @H.confine
    async def f2() :
        assert 101 == H.get('a') and 102 == H.get('b')

        H.let(b = 202)
        res = await f3()
        assert 250 == res
        return res

    @H.confine
    async def f1():
        H.let(a = 101, b = 102)
        return await f2()

    res = await f1()
    assert 250 == res

    assert len(H._frames) == 0 # no confined context left


@pytest.mark.asyncio
async def test_pillar_recursive_async_func():
    # print()
    H = History()

    @H.confine
    async def recursive(n: int):
        assert n + 1 == H.get('a')

        H.let(a = n)
        if n == 0:
            # H.printHist()
            return 200
        else:
            return await recursive(n -1)

    @H.confine
    async def func():
        H.let(a = 4)
        res = await recursive(3)
        assert 200 == res

    await func()

    assert len(H._frames) == 0 # no confined context left


@pytest.mark.asyncio
async def test_pillar_async_generator():
    # print()
    H = History()

    @H.confine
    async def enum_numbers():
        for i in range(H.get('num')):
            await asyncio.sleep(0.001)
            yield i

    @H.confine
    async def func3():
        H.let(num = 5)
        total_sum = 0
        async_generator = enum_numbers()
        async for i in async_generator:
            total_sum += i
        assert 10 == total_sum

    await func3()

    assert len(H._frames) == 0 # no confined context left


@pytest.mark.asyncio
async def test_pillar_async_gen_stop():
    # print()
    H = History()

    @H.confine
    async def enum_numbers():
        for i in range(H.get('num')):
            await asyncio.sleep(0.0001)
            yield i

    @H.confine
    async def func3():
        H.let(num = 3)
        total_sum = 0
        ag = enum_numbers()

        total_sum += await ag.__anext__()
        total_sum += await ag.__anext__()
        total_sum += await ag.__anext__()

        with pytest.raises(StopAsyncIteration):
            total_sum += await ag.__anext__()

        assert 3 == total_sum

    await func3()

    assert len(H._frames) == 0 # no confined context left


@pytest.mark.asyncio
async def test_pillar_async_exit_callback():
    # print()
    H = History()


    async def f1():
        H.let(z = 3)
        assert 1 == H.get('a') and 3 == H.get('z')
        raise ValueError(999)


    exit_callback_called = False

    def exit_callback(exc_type, exc_val, tb):
        nonlocal exit_callback_called
        exit_callback_called = True
        assert exc_type == ValueError

    res = H.confine(f1, a = 1, exit_callback=exit_callback)

    with pytest.raises(ValueError):
        res = await res()

    assert exit_callback_called

    assert len(H._frames) == 0 # no confined context left
