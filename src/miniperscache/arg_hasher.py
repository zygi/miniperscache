import inspect
from typing import Any, Callable
from typing_extensions import ParamSpec
from stablehash import stablehash
from abc import ABC, abstractmethod

Args = ParamSpec("Args")


class MkArgHasherType(ABC):
    """
    A hasher is simply a function Callable[Args, bytes]. Sometimes to build a hasher for a generic function,
    we need to actually access and inspect that function. So, this interface - MkArgHasherType -
    constructs a hasher function for a given function.
    """

    @abstractmethod
    def __call__(self, func: Callable[Args, Any]) -> Callable[Args, bytes]: ...


class DefaultArgHasher(MkArgHasherType):
    def __init__(self, skip_args: list[str] | None = None):
        """
        skip_args: list of argument names of the function to skip from the hashing.
        """
        self.skip_args = skip_args or []

    def __call__(self, func: Callable[..., Any]) -> Callable[..., bytes]:
        sig = inspect.signature(func)

        def hasher(*args, **kwargs) -> bytes:
            bound_args = sig.bind(*args, **kwargs)
            bound_args.apply_defaults()

            # sort by name
            sorted_args = sorted(bound_args.arguments.items(), key=lambda x: x[0])
            digest = stablehash()
            for k, v in sorted_args:
                if k in self.skip_args:
                    continue
                try:
                    digest.update(v)
                except Exception as e:
                    raise ValueError(
                        f"Cannot hash argument name {k} of type {type(v)}"
                    ) from e
            return digest.digest()

        return hasher


def default_raw_arg_hasher(*args, **kwargs) -> bytes:
    "This default hasher is faster but depends on the precise order of the arguments passed. Use it only if you need the performance."
    kwargs_ordered = sorted(kwargs.items(), key=lambda x: x[0])
    sh = stablehash((args, kwargs_ordered))
    digest = sh.digest()
    return digest


def _simple_benchmark():
    import time

    def test_fn_2(x, y, z=4, q=5, *args, **kwargs):
        return x + y + z + q + len(args) + len(kwargs)

    def manual_hasher_without_q(x, y, z=4, q=5, *args, **kwargs):
        # print(x, y, z, q, args, kwargs)
        kwargs_ordered = sorted(kwargs.items(), key=lambda x: x[0])
        sh = stablehash((x, y, q, args, kwargs_ordered))
        return sh.digest()

    auto_hasher_without_q = DefaultArgHasher(["q"])(test_fn_2)

    test_args = (1, 2)
    test_kwargs = {"z": 16, "qq": 4, "q": 6}

    NUM_TIMES = 100000
    # 1) do directly
    start = time.time()
    for i in range(NUM_TIMES):
        manual_hasher_without_q(*test_args, **test_kwargs)
    end = time.time()
    print(f"Direct call: {end - start}")

    # 2) do with hasher
    start = time.time()
    for i in range(NUM_TIMES):
        auto_hasher_without_q(*test_args, **test_kwargs)
    end = time.time()
    print(f"Hashed call: {end - start}")
