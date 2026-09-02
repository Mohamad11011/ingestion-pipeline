from hashing.files import sha256_bytes


def test_same_bytes_produce_the_same_hash() -> None:
    payload = b"decision-body"

    assert sha256_bytes(payload) == sha256_bytes(payload)


def test_changed_bytes_produce_a_different_hash() -> None:
    assert sha256_bytes(b"original") != sha256_bytes(b"changed")


def test_hash_is_lowercase_hex_sha256() -> None:
    digest = sha256_bytes(b"abc")

    assert digest == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    assert len(digest) == 64
