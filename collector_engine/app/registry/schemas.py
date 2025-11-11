from pydantic import BaseModel


class ContractInfo(BaseModel):
    name: str
    abi: str
    address: str
    genesis_block: int


class ProtocolInfo(BaseModel):
    contracts: list[ContractInfo]
