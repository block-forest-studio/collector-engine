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
    data_path: Path = Field(
        default_factory=lambda: Path(__file__).resolve().parents[4] / "data",
        alias="DATA_PATH",
    )
    postgres_dsn: str = Field(..., alias="POSTGRES_DSN")


class Web3Config(BaseConfig):
    """Web3Config."""

    eth_provider_url: str = Field(..., alias="ETH_PROVIDER_URL")
    base_provider_url: str = Field(..., alias="BASE_PROVIDER_URL")

    etherscan_api_key: str = Field(..., alias="ETHERSCAN_API_KEY")
    basescan_api_key: str = Field(..., alias="BASESCAN_API_KEY")

    client_max_concurrency: int = Field(10, alias="CLIENT_MAX_CONCURRENCY")
    client_request_timeout: int = Field(30, alias="CLIENT_REQUEST_TIMEOUT")

    def rpc_url(self, chain_id: int) -> str:
        """Return provider URL based on chain_id."""
        mapping = {
            1: self.eth_provider_url,
            8453: self.base_provider_url,
        }
        try:
            return mapping[chain_id]
        except KeyError:
            raise ValueError(f"No RPC URL defined for chain_id={chain_id}")


app_config: AppConfig = AppConfig()  # type: ignore[call-arg]
web3_config: Web3Config = Web3Config()  # type: ignore[call-arg]
