from __future__ import annotations

import hashlib

HASH_ALGORITHM = "sha256"
HASH_DIGEST_BYTES = 8
ENCODING = "utf-8"


def stable_hash_to_int(key: str) -> int:
    """
    Convert a string key into a stable integer hash.

    Why this exists:
    - Python's built-in hash() is randomized per process for security.
    - For sharding, we need deterministic routing across runs.
    """
    if not isinstance(key, str):
        raise TypeError("key must be a string")
    digest = hashlib.new(HASH_ALGORITHM, key.encode(ENCODING)).digest()
    return int.from_bytes(digest[:HASH_DIGEST_BYTES], "big", signed=False)


def choose_shard(key: str, num_shards: int) -> int:
    if num_shards <= 0:
        raise ValueError("Base Case: Number of shards cannot be 0 or negative")
    
    shard_index = stable_hash_to_int(key) % num_shards

    return shard_index
    


class ShardRouter:
    """
    Small helper that stores 'shards' in a list and chooses one based on a key.

    In the full project, a "shard" could be a DB connection/client.
    In the unit-tested core, a shard can be any object.
    """

    def __init__(self, shards: list[object]):
        if not shards:
            raise ValueError("must provide at least 1 shard")
        self._shards = list(shards)

    @property
    def num_shards(self) -> int:
        return len(self._shards)

    def route(self, key: str) -> object:
        index = choose_shard(key, self.num_shards)
        return self._shards[index]