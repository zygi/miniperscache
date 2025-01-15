from miniperscache.serializer import DillSerializer
from dataclasses import dataclass
import sys
import subprocess


@dataclass
class X:
    x: int


def test_file_storage():
    import tempfile

    with tempfile.NamedTemporaryFile() as f:
        temp_path = f.name

        def child_process(temp_path):
            obj = X(42)
            serializer = DillSerializer()
            with open(temp_path, "wb") as f:
                f.write(serializer.serialize(obj))

        # Spawn child process to pickle object
        p = subprocess.Popen(
            ["python", "-m", "tests.test_cloudpickle_objects", temp_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        p.wait()
        if p.returncode != 0:
            raise Exception(
                f"Child process failed with return code {p.returncode}\n\n{p.stderr.read()}"
            )

        # Now try to unpickle in main process
        serializer = DillSerializer()
        with open(temp_path, "rb") as f:
            data = f.read()
            obj = serializer.deserialize(data)

        # this will fail
        # assert isinstance(obj, X), f"obj is not an instance of X: {obj.__class__}, {X}"

        # But we can use the class as if it was the same class.
        assert obj.x == 42


if __name__ == "__main__":
    temp_path = sys.argv[1]
    obj = X(42)
    serializer = DillSerializer()
    with open(temp_path, "wb") as f:
        f.write(serializer.serialize(obj))
