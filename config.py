import os
from pathlib import Path

from pydantic import BaseModel, Field, ConfigDict, Extra, field_validator, model_validator

from pydantic_settings import SettingsConfigDict, BaseSettings
from typing import Dict, Optional
from .processors import *
from .lag_policies import *
from .utils import import_project_modules


class KafkaPilotSettings(BaseSettings):
    KFP__ENVIRONMENT: str = os.getenv("ENVIRONMENT", "dev")
    KFP__REDIS_HOST: str = os.getenv("KFP__REDIS_HOST")
    KFP__REDIS_PORT: int = os.getenv("KFP__REDIS_PORT")
    KFP__REDIS_DB_NUMBER: int = os.getenv("KFP__REDIS_DB_NUMBER")
    KFP__REDIS_PASSWORD: Optional[str] = None
    KFP__KAFKA_CONF_DIRECTORY: str = os.getenv("KFP__KAFKA_CONF_DIRECTORY")

    dlq_topic_suffix: str = Field(
        default="chr-upn-consumer-dlq",
        description="Suffix for Dead Letter Queue topics"
    )
    model_config = SettingsConfigDict(env_file=".env", extra=Extra.ignore, env_nested_delimiter="__")

    @model_validator(mode="after")
    def validate_conf_dir(self):
        path = Path(self.KFP__KAFKA_CONF_DIRECTORY)
        if not path.is_absolute():
            path = (Path.cwd() / path).expanduser().resolve()
        if not path.exists():
            raise ValueError(f"KAFKA_CONF_DIRECTORY does not exist: {path}")
        self.KFP__KAFKA_CONF_DIRECTORY = str(path)
        import_project_modules(str(path))
        return self


def get_conf():
    return KafkaPilotSettings()
