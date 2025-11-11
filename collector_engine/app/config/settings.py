"""Settings."""

from pathlib import Path
from pydantic import Field
from pydantic_settings import BaseSettings


class BaseConfig(BaseSettings):
    """BaseConfig."""

    class Config:
        """Config class."""

        extra = "ignore"
        env_file = ".env.test", ".env"
        case_sensitive = True


class AppConfig(BaseConfig):
    """AppConfig."""

    project_name: str = Field(..., alias="PROJECT_NAME")
    data_path: Path = Path(__file__).resolve().parent.parent.parent / "data"


class Web3Config(BaseConfig):
    """Web3Config."""

    infura_api_key: str = Field(..., alias="INFURA_API_KEY")
    alchemy_api_key: str = Field(..., alias="ALCHEMY_API_KEY")
    etherscan_api_key: str = Field(..., alias="ETHERSCAN_API_KEY")
    basescan_api_key: str = Field(..., alias="BASESCAN_API_KEY")


app_config: AppConfig = AppConfig()  # type: ignore[call-arg]
web3_config: Web3Config = Web3Config()  # type: ignore[call-arg]
