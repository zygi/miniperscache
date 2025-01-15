import inspect
from typing import Any, Callable, Coroutine, ParamSpec, Sequence, TypeVar

from miniperscache.cache import _check_tag_uniqueness
from miniperscache.serializer import DillSerializer, Serializer
from miniperscache.storage import AsyncStorage, Storage
from miniperscache.arg_hasher import DefaultArgHasher, MkArgHasherType
from miniperscache.storage import SqliteStorage

P = ParamSpec("P")
R = TypeVar("R")


def mk_batch_cached(
    func: Callable[P, Sequence[R]],
    tag: str,
    batch_argument_names: list[str] | None = None,
    arg_hasher: Callable[..., bytes] | MkArgHasherType | None = None,
    value_serializer: Serializer | None = None,
    storage: Storage | None = None,
    force_tag_nonunique: bool = False,
) -> Callable[P, list[R]]:
    """
    Internal function to create a batched cached function. See argument comments in batch_cached() for more details.
    You might want to use this if you want to provide your own arg_hasher and want good typechecker suggestions.
    """
    if not force_tag_nonunique:
        _check_tag_uniqueness(tag)

    if batch_argument_names is None:
        batch_argument_names = []

    # check if func is async, and if so, throw an error. we don't want to cache async functions like this.
    if inspect.iscoroutinefunction(func):
        raise ValueError(
            "In order to cache async functions, you should use batch_cached_async"
        )

    if storage is None:
        storage = SqliteStorage()

    if arg_hasher is None:
        arg_hasher = DefaultArgHasher()
    if isinstance(arg_hasher, MkArgHasherType):
        arg_hasher = arg_hasher(func)

    if value_serializer is None:
        value_serializer = DillSerializer()

    sig = inspect.signature(func)

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> list[R]:
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        # Convert all arguments to keyword arguments
        all_kwargs = bound_args.arguments

        # Split into batch and non-batch args
        batch_args = {k: v for k, v in all_kwargs.items() if k in batch_argument_names}
        non_batch_args = {
            k: v for k, v in all_kwargs.items() if k not in batch_argument_names
        }

        # assert same length
        lengths = []
        for k, v in batch_args.items():
            try:
                lengths.append(len(v))
            except TypeError:
                raise ValueError(f"Batch argument {k} is not iterable")
        if not all(length == lengths[0] for length in lengths):
            raise ValueError(
                f"All batch arguments must have the same length. Found {lengths}"
            )

        propagated_batch_args = {k: [] for k, _ in batch_args.items()}
        propagated_batch_arg_idxs = []

        hashes = []

        results: list[tuple[int, R] | None] = [None] * lengths[0]

        for i in range(lengths[0]):
            call_args = {k: v[i] for k, v in batch_args.items()} | non_batch_args
            # this is a bit of a problem with our design bc the hasher was built to expect lists of things but we're calling it with de-batched args.
            # for now we assume hasher constructions won't depend on types precisely, and just use this.
            key = arg_hasher(**call_args)  # type: ignore
            hashes.append(key)
            value = storage.get(tag, key)
            if value is not None:
                results[i] = (i, value_serializer.deserialize(value))
            else:
                for k, v in propagated_batch_args.items():
                    v.append(call_args[k])
                propagated_batch_arg_idxs.append(i)

        # now, do the non-memoized call
        propagated_args = propagated_batch_args | non_batch_args
        res = func(**propagated_args)  # type: ignore

        assert len(res) == len(
            propagated_batch_arg_idxs
        ), f"Expected {len(propagated_batch_arg_idxs)} results, got {len(res)}"

        # store the new results
        for i, r in zip(propagated_batch_arg_idxs, res):
            storage.set(tag, hashes[i], value_serializer.serialize(r))

        # fill in the results
        for i, r in zip(propagated_batch_arg_idxs, res):
            results[i] = (i, r)

        # assert no zeros
        assert not any(r is None for r in results), f"Found None in results: {results}"

        # sort by index
        results.sort(key=lambda x: x[0])

        return [r[1] for r in results]  # type: ignore

    return wrapper


def batch_cached(
    tag: str,
    batch_argument_names: list[str] | None = None,
    arg_hasher: Callable[..., bytes] | MkArgHasherType | None = None,
    value_serializer: Serializer | None = None,
    storage: Storage | None = None,
    force_tag_nonunique: bool = False,
) -> Callable[[Callable[P, Sequence[R]]], Callable[P, list[R]]]:
    def wrapper(fn: Callable[P, Sequence[R]]) -> Callable[P, list[R]]:
        return mk_batch_cached(
            fn,
            tag,
            batch_argument_names,
            arg_hasher,
            value_serializer,
            storage,
            force_tag_nonunique,
        )

    return wrapper


# async implementation
def mk_async_batch_cached(
    func: Callable[P, Coroutine[Any, Any, Sequence[R]]],
    tag: str,
    batch_argument_names: list[str] | None = None,
    arg_hasher: Callable[..., bytes] | MkArgHasherType | None = None,
    value_serializer: Serializer | None = None,
    storage: Storage | None = None,
    force_tag_nonunique: bool = False,
) -> Callable[P, Coroutine[Any, Any, list[R]]]:
    """Creates an async batched cached function."""

    if storage is None:
        storage = SqliteStorage()
    storage_async = isinstance(storage, AsyncStorage)
    if value_serializer is None:
        value_serializer = DillSerializer()
    if batch_argument_names is None:
        batch_argument_names = []
    if arg_hasher is None:
        arg_hasher = DefaultArgHasher()
    if isinstance(arg_hasher, MkArgHasherType):
        arg_hasher = arg_hasher(func)

    if not force_tag_nonunique:
        _check_tag_uniqueness(tag)

    sig = inspect.signature(func)

    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> list[R]:
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        # Convert all arguments to keyword arguments
        all_kwargs = bound_args.arguments

        # Split into batch and non-batch args
        batch_args = {k: v for k, v in all_kwargs.items() if k in batch_argument_names}
        non_batch_args = {
            k: v for k, v in all_kwargs.items() if k not in batch_argument_names
        }

        # verify all batch args have same length
        lengths = [len(v) for v in batch_args.values()]
        if not lengths:
            return list(await func(*args, **kwargs))
        if not all(length == lengths[0] for length in lengths):
            raise ValueError(
                f"All batch arguments must have same length, got lengths {lengths}"
            )

        # initialize results
        results: list[tuple[int, R] | None] = [None] * lengths[0]

        # initialize propagated args
        propagated_batch_args = {k: [] for k in batch_args}
        propagated_batch_arg_idxs = []
        hashes = []

        # check cache for each item
        for i in range(lengths[0]):
            call_args = {k: v[i] for k, v in batch_args.items()} | non_batch_args
            key = arg_hasher(**call_args)  # type: ignore
            hashes.append(key)
            if storage_async:
                value: bytes | None = await storage.get(tag, key)  # type: ignore
            else:
                value: bytes | None = storage.get(tag, key)  # type: ignore
            if value is not None:
                results[i] = (i, value_serializer.deserialize(value))
            else:
                for k, v in propagated_batch_args.items():
                    v.append(call_args[k])
                propagated_batch_arg_idxs.append(i)

        # now, do the non-memoized call
        propagated_args = propagated_batch_args | non_batch_args
        res = await func(**propagated_args)  # type: ignore

        assert len(res) == len(
            propagated_batch_arg_idxs
        ), f"Expected {len(propagated_batch_arg_idxs)} results, got {len(res)}"

        # store the new results
        for i, r in zip(propagated_batch_arg_idxs, res):
            if storage_async:
                await storage.set(tag, hashes[i], value_serializer.serialize(r))  # type: ignore
            else:
                storage.set(tag, hashes[i], value_serializer.serialize(r))  # type: ignore

        # fill in the results
        for i, r in zip(propagated_batch_arg_idxs, res):
            results[i] = (i, r)

        # assert no zeros
        assert not any(r is None for r in results), f"Found None in results: {results}"

        # sort by index
        results.sort(key=lambda x: x[0])  # type: ignore
        return [r[1] for r in results]  # type: ignore

    return wrapper


def async_batch_cached(
    tag: str,
    batch_argument_names: list[str] | None = None,
    arg_hasher: Callable[..., bytes] | MkArgHasherType | None = None,
    value_serializer: Serializer | None = None,
    storage: Storage | None = None,
    force_tag_nonunique: bool = False,
) -> Callable[
    [Callable[P, Coroutine[Any, Any, Sequence[R]]]],
    Callable[P, Coroutine[Any, Any, list[R]]],
]:
    def wrapper(
        fn: Callable[P, Coroutine[Any, Any, Sequence[R]]],
    ) -> Callable[P, Coroutine[Any, Any, list[R]]]:
        return mk_async_batch_cached(
            fn,
            tag,
            batch_argument_names,
            arg_hasher,
            value_serializer,
            storage,
            force_tag_nonunique,
        )

    return wrapper
