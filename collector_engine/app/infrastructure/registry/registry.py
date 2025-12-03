import os
import yaml
from pathlib import Path
from collector_engine.app.infrastructure.registry.schemas import ProtocolInfo


def get_chains(protocol: str) -> list[int]:
    files = os.listdir(Path(__file__).parent / "protocols" / f"{protocol}")
    return [int(f.removesuffix(".yaml")) for f in files]


def get_protocols() -> list[str]:
    return os.listdir(Path(__file__).parent / "protocols")


def get_protocol_info(chain_id: int, protocol: str) -> ProtocolInfo:
    path = Path(__file__).parent / "protocols" / f"{protocol}" / f"{chain_id}.yaml"
    with path.open() as f:
        data = yaml.safe_load(f)

    return ProtocolInfo(**data)
