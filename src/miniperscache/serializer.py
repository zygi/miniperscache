import json
import cloudpickle
import dill
from typing import Any, Protocol

from . import logging as package_logging

logger = package_logging.getLogger("serializer")


class Serializer(Protocol):
    def serialize(self, value: Any) -> bytes: ...

    def deserialize(self, value: bytes) -> Any: ...


class PickleSerializer(Serializer):
    """
    Serializer that uses Cloudpickle.

    WARNING: Cloudpickle (as well as the built-in pickle module) is not secure. Only unpickle data you trust.
    It is possible to construct malicious pickle data which will execute arbitrary code during unpickling.
    """

    def serialize(self, value: Any) -> bytes:
        return cloudpickle.dumps(value)

    def deserialize(self, value: bytes) -> Any:
        return cloudpickle.loads(value)


class DillSerializer(Serializer):
    def serialize(self, value: Any) -> bytes:
        return dill.dumps(value)

    def deserialize(self, value: bytes) -> Any:
        return dill.loads(value)


class JsonSerializer(Serializer):
    def serialize(self, value: Any) -> bytes:
        return json.dumps(value).encode("utf-8")

    def deserialize(self, value: bytes) -> Any:
        return json.loads(value.decode("utf-8"))
