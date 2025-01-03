import asyncio
from typing import Any, Callable
from miniperscache.arg_hasher import MkArgHasherType
from miniperscache.cache import cached, cached_async
from miniperscache.storage import SqliteStorage, Storage, AsyncStorage
import pytest


def test_mk_cached(storage_class: type[Storage] = SqliteStorage):
    mutating_counter = 0

    storage = storage_class()
    storage.delete_with_tag("test_mk_cached")

    @cached(storage=storage, tag="test_mk_cached")
    def test_func(a: int, b: int) -> int:
        nonlocal mutating_counter
        mutating_counter += 1
        return a + b

    assert test_func(1, 2) == 3
    assert mutating_counter == 1, f"mutating_counter is {mutating_counter}"
    assert test_func(1, 2) == 3
    assert mutating_counter == 1, f"mutating_counter is {mutating_counter}"
    assert test_func(5, 2) == 7
    assert mutating_counter == 2, f"mutating_counter is {mutating_counter}"


@pytest.mark.asyncio
async def test_mk_cached_async(
    storage_class: type[Storage | AsyncStorage] = SqliteStorage,
):
    mutating_counter = 0

    storage = storage_class()
    if isinstance(storage, AsyncStorage):
        await storage.delete_with_tag("test_mk_cached_async")
    else:
        storage.delete_with_tag("test_mk_cached_async")

    @cached_async(storage=storage, tag="test_mk_cached_async")
    async def test_func(a: int, b: int) -> int:
        nonlocal mutating_counter
        mutating_counter += 1
        return a + b

    assert await test_func(1, 2) == 3
    assert mutating_counter == 1, f"mutating_counter is {mutating_counter}"
    assert await test_func(1, 2) == 3
    assert mutating_counter == 1, f"mutating_counter is {mutating_counter}"
    assert await test_func(5, 2) == 7
    assert mutating_counter == 2, f"mutating_counter is {mutating_counter}"


def test_kwarg_order():
    storage = SqliteStorage()
    storage.delete_with_tag("test_kwarg_order")

    mutating_counter = 0

    @cached(storage=storage, tag="test_kwarg_order")
    def test_func(a: int, b: int) -> int:
        nonlocal mutating_counter
        mutating_counter += 1
        return a + b

    assert test_func(a=1, b=2) == 3
    assert mutating_counter == 1, f"mutating_counter is {mutating_counter}"
    assert test_func(b=2, a=1) == 3
    assert mutating_counter == 1, f"mutating_counter is {mutating_counter}"


def test_async_error():
    ok = False
    try:

        @cached("test_async_error")
        async def test_func() -> int:
            return 5
    except ValueError:
        ok = True
    assert ok, "Expected ValueError"


@pytest.mark.asyncio
async def test_async_is_async():
    from miniperscache.storage import AsyncFileStorage

    storage = AsyncFileStorage()
    await storage.delete_with_tag("test_async_is_async")
    cond = asyncio.Condition()

    mutable_counter = 0
    num_tasks = 2

    @cached_async(storage=storage, tag="test_async_is_async")
    async def test_func() -> int:
        nonlocal mutable_counter
        mutable_counter += 1
        async with cond:
            if mutable_counter < num_tasks:
                await cond.wait()
            else:
                cond.notify_all()
        return 5

    tasks = [asyncio.create_task(test_func()) for _ in range(num_tasks)]

    # Run with timeout. If the cache somehow serializes things and doesn't allow two tasks to run concurrently, this will time out because
    # the condition will never be notified.
    try:
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)
    except asyncio.TimeoutError:
        return

    assert results == [5, 5], f"results are {results}"


def test_nonunique_tag():
    @cached("tag")
    def test_func():
        return 5

    try:

        @cached("tag")
        def test_func2():
            return 5

        assert False
    except ValueError:
        pass

    @cached("tag", force_tag_nonunique=True)
    def test_func3():
        return 5

    assert test_func3() == 5


def test_custom_arg_hasher():
    storage = SqliteStorage()
    storage.delete_with_tag("test_custom_hasher")

    def custom_hasher(*args, **kwargs) -> bytes:
        # Simple hasher that only looks at first arg
        return str(args[0]).encode("utf-8")

    @cached(tag="test_custom_hasher", arg_hasher=custom_hasher)
    def test_func(x: int, y: int) -> int:
        return x + y

    # Should cache based only on x
    assert test_func(1, 1) == 2
    assert test_func(1, 2) == 2  # Returns cached value even though y changed
    assert test_func(2, 1) == 3  # New cache entry for x=2
    assert test_func(2, 2) == 3  # Returns cached value for x=2

    # Test with MkArgHasherType
    storage.delete_with_tag("test_mk_hasher")

    class MkFirstArgHasher(MkArgHasherType):
        def __call__(self, func: Callable[..., Any]) -> Callable[..., bytes]:
            def hasher(*args, **kwargs) -> bytes:
                return str(args[0]).encode("utf-8")

            return hasher

    @cached(tag="test_mk_hasher", arg_hasher=MkFirstArgHasher())
    def test_func2(x: int, y: int) -> int:
        return x + y

    assert test_func2(1, 1) == 2
    assert test_func2(1, 2) == 2  # Returns cached value
    assert test_func2(2, 1) == 3  # New cache entry
    assert test_func2(2, 2) == 3  # Returns cached value
