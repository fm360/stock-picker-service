from stockpicker.sharding import choose_shard, stable_hash_to_int


def test_stable_hash_is_stable():
    assert stable_hash_to_int("abc") == stable_hash_to_int("abc")


def test_choose_shard_range():
    s = choose_shard("user:u_001", 3)
    assert 0 <= s < 3


def test_choose_shard_deterministic():
    assert choose_shard("user:u_001", 3) == choose_shard("user:u_001", 3)



