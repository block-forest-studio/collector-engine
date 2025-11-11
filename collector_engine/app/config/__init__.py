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

    INFURA_API_KEY: str
    ALCHEMY_API_KEY: str
    ETHERSCAN_API_KEY: str
    BASESCAN_API_KEY: str


app_config: AppConfig = AppConfig()
web3_config: Web3Config = Web3Config()
