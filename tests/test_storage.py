import pytest
from miniperscache.storage import FileStorage, SqliteStorage, AsyncFileStorage


@pytest.fixture
def temp_path(tmp_path):
    return tmp_path / "miniperscache_test"


def test_file_storage(temp_path):
    storage = FileStorage(temp_path)
    storage.delete_with_tag("test_file_storage")

    # Test set and get
    storage.set("test_tag", b"key1", b"value1")
    assert storage.get("test_tag", b"key1") == b"value1"

    # Test get nonexistent
    assert storage.get("test_tag", b"nonexistent") is None

    # Test overwrite
    storage.set("test_tag", b"key1", b"value2")
    assert storage.get("test_tag", b"key1") == b"value2"

    # Test delete tag
    storage.set("test_tag2", b"key2", b"value3")
    storage.delete_with_tag("test_tag")
    assert storage.get("test_tag", b"key1") is None
    assert storage.get("test_tag2", b"key2") == b"value3"


def test_sqlite_storage(temp_path):
    db_path = temp_path / "test.db"
    storage = SqliteStorage(db_path)
    storage.delete_with_tag("test_sqlite_storage")

    # Test set and get
    storage.set("test_tag", b"key1", b"value1")
    assert storage.get("test_tag", b"key1") == b"value1"

    # Test get nonexistent
    assert storage.get("test_tag", b"nonexistent") is None

    # Test overwrite
    storage.set("test_tag", b"key1", b"value2")
    assert storage.get("test_tag", b"key1") == b"value2"

    # Test delete tag
    storage.set("test_tag2", b"key2", b"value3")
    storage.delete_with_tag("test_tag")
    assert storage.get("test_tag", b"key1") is None
    assert storage.get("test_tag2", b"key2") == b"value3"


@pytest.mark.asyncio
async def test_async_file_storage(temp_path):
    storage = AsyncFileStorage(temp_path)
    await storage.delete_with_tag("test_async_file_storage")
    # Test set and get
    await storage.set("test_tag", b"key1", b"value1")
    assert await storage.get("test_tag", b"key1") == b"value1"

    # Test get nonexistent
    assert await storage.get("test_tag", b"nonexistent") is None

    # Test overwrite
    await storage.set("test_tag", b"key1", b"value2")
    assert await storage.get("test_tag", b"key1") == b"value2"

    # Test delete tag
    await storage.set("test_tag2", b"key2", b"value3")
    await storage.delete_with_tag("test_tag")
    assert await storage.get("test_tag", b"key1") is None
    assert await storage.get("test_tag2", b"key2") == b"value3"
