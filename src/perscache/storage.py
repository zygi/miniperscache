from abc import ABC, abstractmethod
import base64
import pathlib
import sqlite3
from . import logging as package_logging
import asyncio

logger = package_logging.getLogger("storage")


def _get_default_path(provided: str | pathlib.Path | None) -> pathlib.Path:
    if provided is None:
        return pathlib.Path.cwd() / ".perscache"
    path = pathlib.Path(provided)
    path.mkdir(parents=True, exist_ok=True)
    return path


class Storage(ABC):
    @abstractmethod
    def get(self, tag: str, key: bytes) -> bytes | None: ...

    @abstractmethod
    def set(self, tag: str, key: bytes, value: bytes) -> None: ...

    @abstractmethod
    def delete_with_tag(self, tag: str) -> None: ...


class FileStorage(Storage):
    def __init__(self, path: str | pathlib.Path | None = None):
        self.path = _get_default_path(path)
        logger.info("Perscache file storage initialized at %s", self.path)

    def _mk_file_name(self, tag: str, key: bytes) -> pathlib.Path:
        key_base64 = base64.urlsafe_b64encode(key).decode("utf-8")
        return self.path / tag / key_base64

    def get(self, tag: str, key: bytes) -> bytes | None:
        file_name = self._mk_file_name(tag, key)
        logger.debug("Getting value from %s", file_name)
        if not file_name.exists():
            return None
        with open(file_name, "rb") as f:
            return f.read()

    def set(self, tag: str, key: bytes, value: bytes) -> None:
        file_name = self._mk_file_name(tag, key)
        file_name.parent.mkdir(parents=True, exist_ok=True)
        logger.debug("Setting value to %s", file_name)
        with open(file_name, "wb") as f:
            f.write(value)

    def delete_with_tag(self, tag: str) -> None:
        for file in self.path.glob(f"{tag}/*"):
            file.unlink()


class SqliteStorage(Storage):
    def __init__(self, db_path: str | pathlib.Path | None = None):
        if db_path is None:
            db_path = _get_default_path(None) / "perscache.db"
        if isinstance(db_path, str):
            db_path = pathlib.Path(db_path)
        # Create parent directory if it doesn't exist
        db_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info("Perscache sqlite storage initialized at %s", db_path)

        self.conn = sqlite3.connect(db_path)
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS cache (tag TEXT, key BLOB, value BLOB)"
        )
        # add a primary key index on tag and key
        self.conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS cache_tag_key_idx ON cache (tag, key)"
        )
        self.conn.commit()

    def get(self, tag: str, key: bytes) -> bytes | None:
        cursor = self.conn.execute(
            "SELECT value FROM cache WHERE tag = ? AND key = ?", (tag, key)
        )
        row = cursor.fetchone()
        return row[0] if row else None

    def set(self, tag: str, key: bytes, value: bytes) -> None:
        self.conn.execute(
            "INSERT INTO cache (tag, key, value) VALUES (?, ?, ?) ON CONFLICT (tag, key) DO UPDATE SET value = ?",
            (tag, key, value, value),
        )
        self.conn.commit()

    def delete_with_tag(self, tag: str) -> None:
        self.conn.execute("DELETE FROM cache WHERE tag = ?", (tag,))
        self.conn.commit()


class AsyncStorage(ABC):
    @abstractmethod
    async def get(self, tag: str, key: bytes) -> bytes | None: ...

    @abstractmethod
    async def set(self, tag: str, key: bytes, value: bytes) -> None: ...

    @abstractmethod
    async def delete_with_tag(self, tag: str) -> None: ...


class AsyncFileStorage(AsyncStorage):
    def __init__(self, path: str | pathlib.Path | None = None):
        # just wraps FileStorage
        self.storage = FileStorage(path)

    async def get(self, tag: str, key: bytes) -> bytes | None:
        return await asyncio.to_thread(self.storage.get, tag, key)

    async def set(self, tag: str, key: bytes, value: bytes) -> None:
        return await asyncio.to_thread(self.storage.set, tag, key, value)

    async def delete_with_tag(self, tag: str) -> None:
        return await asyncio.to_thread(self.storage.delete_with_tag, tag)
