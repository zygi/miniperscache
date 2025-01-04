from miniperscache.batched_cache import batch_cached, async_batch_cached
from miniperscache.storage import SqliteStorage
import pytest


def test_batch_cached():
    storage = SqliteStorage()
    storage.delete_with_tag("test")

    mutable_edited_items_counter = 0

    @batch_cached(tag="test", batch_argument_names=["a"], storage=storage)
    def f(a: list[int], b: int) -> list[int]:
        nonlocal mutable_edited_items_counter
        mutable_edited_items_counter += len(a)
        return [ai + b for ai in a]

    assert f([1, 2, 3], 1) == [2, 3, 4], f"Expected [2, 3, 4], got {f([1, 2, 3], 1)}"
    assert mutable_edited_items_counter == 3

    assert f([4, 5, 6], 1) == [5, 6, 7], f"Expected [5, 6, 7], got {f([4, 5, 6], 1)}"
    assert mutable_edited_items_counter == 6

    assert f([1, 5, 9], 1) == [2, 6, 10], f"Expected [2, 6, 10], got {f([1, 5, 9], 1)}"
    assert mutable_edited_items_counter == 7


@pytest.mark.asyncio
async def test_async_batch_cached():
    storage = SqliteStorage()
    storage.delete_with_tag("test2")

    mutable_edited_items_counter = 0

    @async_batch_cached(tag="test2", batch_argument_names=["a"], storage=storage)
    async def f(a: list[int], b: int) -> list[int]:
        nonlocal mutable_edited_items_counter
        mutable_edited_items_counter += len(a)
        return [ai + b for ai in a]

    assert await f([1, 2, 3], 1) == [
        2,
        3,
        4,
    ], f"Expected [2, 3, 4], got {await f([1, 2, 3], 1)}"
    assert (
        mutable_edited_items_counter == 3
    ), f"Expected 3, got {mutable_edited_items_counter}"

    assert await f([4, 5, 6], 1) == [
        5,
        6,
        7,
    ], f"Expected [5, 6, 7], got {await f([4, 5, 6], 1)}"
    assert (
        mutable_edited_items_counter == 6
    ), f"Expected 6, got {mutable_edited_items_counter}"

    assert await f([1, 5, 9], 1) == [
        2,
        6,
        10,
    ], f"Expected [2, 6, 10], got {await f([1, 5, 9], 1)}"
    assert (
        mutable_edited_items_counter == 7
    ), f"Expected 7, got {mutable_edited_items_counter}"
