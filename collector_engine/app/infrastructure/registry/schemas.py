from pydantic import BaseModel, field_validator


class ContractInfo(BaseModel):
    name: str
    abi: str
    address: bytes
    genesis_block: int

    @field_validator("address", mode="before")
    @classmethod
    def hex_str_to_bytes(cls, v: str | bytes) -> bytes:
        if isinstance(v, str) and v.startswith("0x"):
            return bytes.fromhex(v[2:])
        if isinstance(v, str):
            # brak prefixu - może być błąd użytkownika
            raise ValueError(f"Address must start with '0x': {v}")
        if isinstance(v, bytes):
            return v
        raise TypeError("address must be hex string or bytes")


class ProtocolInfo(BaseModel):
    contracts: list[ContractInfo]
