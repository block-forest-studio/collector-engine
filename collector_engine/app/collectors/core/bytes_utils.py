from hexbytes import HexBytes
from typing import Callable


BytesLike = bytes | bytearray | memoryview


def to_bytes(x: BytesLike | HexBytes | str | None) -> bytes | None:
    """Convert hex str/HexBytes/bytes-like to bytes. None -> None."""
    if x is None:
        return None
    if isinstance(x, (bytes, bytearray, memoryview, HexBytes)):
        return bytes(x)
    if isinstance(x, str):
        s = x[2:] if x.startswith("0x") else x
        return bytes.fromhex(s)
    if isinstance(x, int):
        length = (x.bit_length() + 7) // 8
        return x.to_bytes(length or 1, byteorder="big")
    return bytes(x)


def make_bytes_validator(expected_length: int) -> Callable[[BytesLike | None, str], bytes | None]:
    """Returns a function that validates the length of a bytes-like value is equal to `expected_length`."""

    def b_validate(value: BytesLike | None, field: str) -> bytes | None:
        if value is None:
            return None
        bb = bytes(value)  # memoryview/bytearray -> bytes
        if len(bb) != expected_length:
            raise ValueError(f"{field}: expected {expected_length} bytes, got {len(bb)}")
        return bb

    return b_validate


# domain aliases
b32_validate = make_bytes_validator(32)
b20_validate = make_bytes_validator(20)
b256_validate = make_bytes_validator(256)
