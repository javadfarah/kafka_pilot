from pathlib import Path

from pydantic import AliasChoices, Field, Extra, model_validator

from pydantic_settings import SettingsConfigDict, BaseSettings
from typing import Optional
from .processors import *
from .lag_policies import *
from .utils import import_project_modules


class KafkaPilotSettings(BaseSettings):
    """Reads settings from process environment only (no project .env path).

    Host apps must load env vars (e.g. dotenv) before constructing settings.
    """
    KFP__ENVIRONMENT: str = "dev"
    KFP__REDIS_HOST: str
    KFP__REDIS_PORT: int
    KFP__REDIS_DB_NUMBER: int
    KFP__REDIS_PASSWORD: Optional[str] = None
    KFP__REDIS_USERNAME: Optional[str] = None
    # When True, KFP__REDIS_HOST/PORT are Sentinel endpoints; master is discovered.
    KFP__REDIS_USE_SENTINEL: bool = False
    KFP__REDIS_SENTINEL_MASTER_NAME: Optional[str] = None
    # Optional extra sentinels as "host1:port1,host2:port2". Empty → only HOST:PORT.
    KFP__REDIS_SENTINEL_HOSTS: Optional[str] = None
    # Auth for Sentinel itself. Sentinel authenticates separately from the data node,
    # so leave these unset when it accepts unauthenticated clients.
    KFP__REDIS_SENTINEL_USERNAME: Optional[str] = None
    KFP__REDIS_SENTINEL_PASSWORD: Optional[str] = None
    # Namespace for every key this package writes. Reuses the host app's
    # REDIS_KEY_PREFIX when no kafka_pilot-specific value is given.
    KFP__REDIS_KEY_PREFIX: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("KFP__REDIS_KEY_PREFIX", "REDIS_KEY_PREFIX"),
    )
    KFP__KAFKA_CONF_DIRECTORY: str

    dlq_topic_suffix: str = Field(
        default="chr-upn-consumer-dlq",
        description="Suffix for Dead Letter Queue topics"
    )
    model_config = SettingsConfigDict(extra=Extra.ignore, env_nested_delimiter="__",env_file=".env")

    @model_validator(mode="after")
    def validate_conf_dir(self):
        path = Path(self.KFP__KAFKA_CONF_DIRECTORY)
        if not path.is_absolute():
            path = (Path.cwd() / path).expanduser().resolve()
        if not path.exists():
            raise ValueError(f"KAFKA_CONF_DIRECTORY does not exist: {path}")
        self.KFP__KAFKA_CONF_DIRECTORY = str(path)
        if self.KFP__REDIS_USE_SENTINEL and not (self.KFP__REDIS_SENTINEL_MASTER_NAME or "").strip():
            raise ValueError(
                "KFP__REDIS_SENTINEL_MASTER_NAME is required when KFP__REDIS_USE_SENTINEL=true"
            )
        prefix = (self.KFP__REDIS_KEY_PREFIX or "").strip()
        if prefix and not prefix.endswith("_"):
            prefix += "_"
        self.KFP__REDIS_KEY_PREFIX = prefix
        import_project_modules(str(path))
        return self

    def sentinel_addresses(self) -> list[tuple[str, int]]:
        raw = (self.KFP__REDIS_SENTINEL_HOSTS or "").strip()
        if not raw:
            return [(self.KFP__REDIS_HOST, int(self.KFP__REDIS_PORT))]
        nodes: list[tuple[str, int]] = []
        for part in raw.split(","):
            part = part.strip()
            if not part:
                continue
            host, _, port = part.partition(":")
            if not host or not port:
                raise ValueError(
                    f"Invalid KFP__REDIS_SENTINEL_HOSTS entry {part!r}; expected host:port"
                )
            nodes.append((host.strip(), int(port.strip())))
        return nodes or [(self.KFP__REDIS_HOST, int(self.KFP__REDIS_PORT))]


def get_conf():
    return KafkaPilotSettings()
