import pytest

from hexbytes import HexBytes
from collector_engine.app.collectors.core.bytes_utils import (
    b20_validate,
    b32_validate,
    b256_validate,
    convert_hex_to_bytes,
)

FIELD = "block_hash"
B20 = 20
B32 = 32
B256 = 256


@pytest.mark.parametrize(
    "validator, expected_length",
    [
        (b20_validate, B20),
        (b32_validate, B32),
        (b256_validate, B256),
    ],
)
def test_bytes_validate__success(validator, expected_length):
    data = b"\x00" * expected_length

    out = validator(data, FIELD)

    assert out == data
    assert isinstance(out, bytes)


@pytest.mark.parametrize(
    "validator, expected_length",
    [
        (b20_validate, B20),
        (b32_validate, B32),
        (b256_validate, B256),
    ],
)
def test_byteslike_validate__success(validator, expected_length):
    data = bytearray(b"\x00" * expected_length)

    out = validator(data, FIELD)

    assert out == data
    assert isinstance(out, bytes)


@pytest.mark.parametrize(
    "validator",
    [(b20_validate), (b32_validate), (b256_validate)],
)
def test_none_validate_success(validator):
    assert validator(None, FIELD) is None


@pytest.mark.parametrize(
    "validator, expected_length, bad_length",
    [
        (b20_validate, B20, B20 - 1),
        (b32_validate, B32, B32 - 1),
        (b256_validate, B256, B256 - 1),
    ],
)
def test_wrong_length_validate__error(validator, expected_length, bad_length):
    data = b"\x00" * bad_length

    with pytest.raises(ValueError) as exc:
        validator(data, FIELD)

    assert str(exc.value) == f"{FIELD}: expected {expected_length} bytes, got {bad_length}"


def test_hex_to_bytes_none__success():
    assert convert_hex_to_bytes(None) is None


@pytest.mark.parametrize(
    "input,expected",
    [
        (b"\x01\x02", b"\x01\x02"),  # bytes
        (bytearray(b"\x01\x02"), b"\x01\x02"),  # bytearray -> bytes
        (memoryview(b"\x01\x02"), b"\x01\x02"),  # memoryview -> bytes
        (HexBytes(b"\x01\x02"), b"\x01\x02"),  # HexBytes -> bytes
        ("0x0102", b"\x01\x02"),  # hex str with 0x
        ("0102", b"\x01\x02"),  # hex str without 0x
        ("0x0A0b", b"\x0a\x0b"),  # case-insensitive
        ("0a 0b", b"\x0a\x0b"),  # spaces tolerated by fromhex
    ],
)
def test_hex_to_bytes__success(input, expected):
    result = convert_hex_to_bytes(input)

    assert result == expected
    assert isinstance(result, bytes)
