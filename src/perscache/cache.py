import pickle
from typing import Any, Coroutine
from typing_extensions import Callable, TypeVar, ParamSpec

from perscache.arg_hasher import DefaultArgHasher, MkArgHasherType
from perscache.serializer import PickleSerializer, Serializer
from perscache.storage import AsyncStorage, Storage, SqliteStorage
from . import logging as package_logging
import inspect

P = ParamSpec("P")
R = TypeVar("R")

_logger = package_logging.getLogger()

# we check that tags are unique within the runtime of the program, to prevent copy-paste errors.
_TAG_REGISTRY = {}


def _check_tag_uniqueness(tag: str) -> None:
    if tag in _TAG_REGISTRY:
        raise ValueError(
            f"Perscache tag {tag} already registered for another function. Please use a unique tag for each function. If you are trying to cache a function that is called multiple times, you can use the `force_tag_nonunique` flag to bypass this check."
        )
    _TAG_REGISTRY[tag] = True


def mk_cached(
    func: Callable[P, R],
    tag: str,
    arg_hasher: Callable[P, bytes] | MkArgHasherType | None = None,
    value_serializer: Serializer | None = None,
    storage: Storage | None = None,
    force_tag_nonunique: bool = False,
) -> Callable[P, R]:
    """
    Internal function to create a cached function. See argument comments in cached() for more details.
    You might want to use this if you want to provide your own arg_hasher and want good typechecker suggestions.
    """
    if not force_tag_nonunique:
        _check_tag_uniqueness(tag)

    # check if func is async, and if so, throw an error. we don't want to cache async functions like this.
    if inspect.iscoroutinefunction(func):
        raise ValueError(
            "In order to cache async functions, you should use cached_async"
        )

    if storage is None:
        storage = SqliteStorage()

    if arg_hasher is None:
        arg_hasher = DefaultArgHasher()
    if isinstance(arg_hasher, MkArgHasherType):
        arg_hasher = arg_hasher(func)

    if value_serializer is None:
        value_serializer = PickleSerializer()

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        key = arg_hasher(*args, **kwargs)
        value = storage.get(tag, key)
        if value is not None:
            _logger.debug("Cache hit for %s with key %s", tag, key)
            return value_serializer.deserialize(value)
        _logger.debug("Cache miss for %s with key %s", tag, key)
        result = func(*args, **kwargs)
        storage.set(tag, key, pickle.dumps(result))
        return result

    return wrapper


# the decorator
def cached(
    tag: str,
    arg_hasher: Callable[P, bytes] | MkArgHasherType | None = None,
    value_serializer: Serializer | None = None,
    storage: Storage | None = None,
    force_tag_nonunique: bool = False,
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Decorator to cache function results persistently.

    Args:
        tag: A unique string identifier for this cached function
        arg_hasher: Optional function to generate cache keys from function arguments.
                   If None, uses DefaultArgHasher()
        value_serializer: Optional serializer for cache values. If None, uses PickleSerializer
        storage: Optional storage backend. If None, uses SqliteStorage
        force_tag_nonunique: If True, allows reusing the same tag for multiple functions

    Returns:
        A decorator that will cache the decorated function's results

    Example:
        @cached("my_expensive_func")
        def expensive_calculation(x: int, y: int) -> float:
            # Results will be cached based on x and y arguments
            return complex_math(x, y)

    Raises:
        ValueError: If tag is already used by another cached function and force_tag_nonunique=False
    """

    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        return mk_cached(
            func, tag, arg_hasher, value_serializer, storage, force_tag_nonunique
        )

    return decorator


def mk_cached_async(
    func: Callable[P, Coroutine[Any, Any, R]],
    tag: str,
    arg_hasher: Callable[P, bytes] | MkArgHasherType | None = None,
    value_serializer: Serializer | None = None,
    storage: Storage | AsyncStorage | None = None,
    force_tag_nonunique: bool = False,
) -> Callable[P, Coroutine[Any, Any, R]]:
    """
    Internal function to create a cached async function. See argument comments in cached_async() for more details.
    You might want to use this if you want to provide your own arg_hasher and want good typechecker suggestions.
    """
    if not force_tag_nonunique:
        _check_tag_uniqueness(tag)

    if storage is None:
        storage = SqliteStorage()

    if arg_hasher is None:
        arg_hasher = DefaultArgHasher()
    if isinstance(arg_hasher, MkArgHasherType):
        arg_hasher = arg_hasher(func)

    if value_serializer is None:
        value_serializer = PickleSerializer()

    if isinstance(storage, AsyncStorage):

        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            key = arg_hasher(*args, **kwargs)
            value = await storage.get(tag, key)
            if value is not None:
                _logger.debug("Cache hit for %s with key %s", tag, key)
                return value_serializer.deserialize(value)
            _logger.debug("Cache miss for %s with key %s", tag, key)
            result = await func(*args, **kwargs)

            await storage.set(tag, key, pickle.dumps(result))
            return result
    else:

        async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            key = arg_hasher(*args, **kwargs)
            value = storage.get(tag, key)
            if value is not None:
                _logger.debug("Cache hit for %s with key %s", tag, key)
                return value_serializer.deserialize(value)
            _logger.debug("Cache miss for %s with key %s", tag, key)
            result = await func(*args, **kwargs)
            storage.set(tag, key, pickle.dumps(result))
            return result

    return wrapper


def cached_async(
    tag: str,
    arg_hasher: Callable[P, bytes] | MkArgHasherType | None = None,
    value_serializer: Serializer | None = None,
    storage: AsyncStorage | Storage | None = None,
    force_tag_nonunique: bool = False,
) -> Callable[
    [Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]
]:
    """
    Decorator to cache an async function. The function's arguments are hashed to create a key, and the return value is serialized and stored.

    Args:
        tag: A unique tag for this cached function. Used to clear all cached values for this function.
        arg_hasher: Optional function to hash the arguments into a key. If None, uses DefaultArgHasher().
        value_serializer: Optional serializer for the return value. If None, uses PickleSerializer().
        storage: Optional storage backend. If None, uses SqliteStorage().
        force_tag_nonunique: If True, allows multiple functions to share the same tag.
    """

    def decorator(
        func: Callable[P, Coroutine[Any, Any, R]],
    ) -> Callable[P, Coroutine[Any, Any, R]]:
        return mk_cached_async(
            func, tag, arg_hasher, value_serializer, storage, force_tag_nonunique
        )

    return decorator
