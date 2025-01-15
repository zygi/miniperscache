![pypi](https://img.shields.io/pypi/v/miniperscache)

A small python package for persistent caching of function results. Main features:
- Supports both sync and async functions.
- Properly typed, so your decorated functions will have the same type hints as the original function.
- Customizable serializers and storage backends.

Unsupported, todo:
- TTL/expiry
- Class/instance methods

## Examples

### Sync
```python
from miniperscache import cached

@cached(tag="expensive_calculation")
def expensive_calculation(x: int, y: int) -> float:
    return x * y
```

### Async
```python
from miniperscache import cached_async

@cached_async(tag="expensive_calculation")
async def expensive_calculation(x: int, y: int) -> float:
    result = await fetch_expensive_data(x, y)
    return result
```

### Custom serializers and storage
```python
from miniperscache import cached, FileStorage, JsonSerializer

@cached(tag="expensive_calculation", storage=FileStorage(), value_serializer=JsonSerializer())
def expensive_calculation(x: int, y: int) -> float:
    ...
```

### Skipping unhashable arguments
The cache works by hashing the arguments of the function call. If some of the arguments, e.g. random Python objects, are unhashable, you can skip them by passing a custom arg hasher set to `DefaultArgHasher(skip_args=["arg_names", ...])`:
```python
from miniperscache import cached_async, DefaultArgHasher

@cached_async(tag="expensive_calculation", arg_hasher=DefaultArgHasher(skip_args=["client"]))
async def model_call(client: ClientObject, prompt: str) -> float:
    res = await client.messages.create(
        prompt=prompt
    )
    return res.content
```

Or, you can provide your own arg hasher
```python
from miniperscache import cached_async, default_raw_arg_hasher

def model_call_arg_hasher(client: ClientObject, prompt: str) -> bytes:
    return default_raw_arg_hasher(client.model, prompt)

@cached_async(tag="expensive_calculation", arg_hasher=model_call_arg_hasher)
async def model_call(client: ClientObject, prompt: str) -> float:
    res = await client.messages.create(
        ...
        prompt=prompt
    )
    return res.content
```

### Deletion
TTL / more systematic expiration isn't supported yet, but what you can do is:
```python
from miniperscache import SqliteStorage
SqliteStorage().delete_tag("expensive_calculation")
```

## Customization
### Serializers
Currently:
- DillSerializer (default)
- PickleSerializer
- JsonSerializer

### Storage
- SqliteStorage (default)
-- Note: serializing to/from disk should be very fast so we don't even provide AsyncSqliteStorage - if writing to disk becomes a bottleneck for you, you can implement it using aiosqlite.
- FileStorage
- AsyncFileStorage
