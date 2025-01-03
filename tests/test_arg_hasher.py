from perscache.arg_hasher import DefaultArgHasher


def test_default_skip_names():
    def test_fn(x, y, z=4, q=5, *args, **kwargs):
        return x + y + z + q + len(args) + len(kwargs)

    hasher = DefaultArgHasher(["q"])(test_fn)

    base_args = {"x": 1, "y": 2, "z": 3}
    hash_1 = hasher(**base_args)
    hash_2 = hasher(**base_args | {"q": 5})
    hash_3 = hasher(**base_args | {"q": 6})
    hash_4 = hasher(**base_args | {"q": 7})
    assert hash_1 == hash_2, f"hash_1: {hash_1}, hash_2: {hash_2}"
    assert hash_1 == hash_3, f"hash_1: {hash_1}, hash_3: {hash_3}"
    assert hash_1 == hash_4, f"hash_1: {hash_1}, hash_4: {hash_4}"

    # different order
    base_args_diff = dict(reversed(base_args.items()))
    hash_5 = hasher(**base_args_diff)
    assert hash_1 == hash_5, f"hash_1: {hash_1}, hash_5: {hash_5}"

    # different args
    hash_6 = hasher(**base_args | {"y": 10})
    assert hash_1 != hash_6, f"hash_1: {hash_1}, hash_6: {hash_6}"


def test_unhashable_error_msg():
    class Unhashable:
        pass

    def test_fn(a, arg_name_b):
        return a + arg_name_b

    hasher = DefaultArgHasher()(test_fn)
    try:
        hasher(1, arg_name_b=Unhashable())
    except ValueError as e:
        assert "arg_name_b" in str(
            e
        ), f"Error message does not contain 'arg_name_b': {str(e)}"
        return
    raise Exception("No error raised")
