import os

from pydantic import BaseModel, ConfigDict, Field, model_validator


class TopicConfig(BaseModel):
    """Configuration for a single Kafka topic."""
    topics: str = Field(..., description="Kafka topics name")
    max_retries: int = Field(default=3, description="Maximum retry attempts for failed messages")
    backoff_ms: int = Field(default=1000, description="Initial backoff time in milliseconds")
    backoff_multiplier: float = Field(default=2.0, description="Backoff multiplier for exponential retry")
    processing_timeout_s: int = Field(default=30, description="Processing timeout in seconds")
    lag_policy: str = Field(default=None, description="Lag policy method")
    model_config = ConfigDict(arbitrary_types_allowed=True)
    isolation_level: str = Field(default="read_committed")
    auto_commit: bool = Field(default=False)
    acceptable_lag: int = Field(default=None,
                                description="Maximum number of leg between consumer and last produced message")
    kafka_bootstrap_servers: str = Field(default=None, description="kafka bootstrap servers")
    sasl_mechanism: str = Field(default=None, description="kafka sasl mechanism")
    security_protocol: str = Field(default=None, description="kafka security protocol")
    kafka_username: str = Field(default=None, description="kafka username")
    kafka_password: str = Field(default=None, description="kafka password")
    kafka_group_id: str = Field(
        default=f"chr-upn-consumer-kafka-worker-group{os.getenv('KFP__ENVIRONMENT')}",
        description="Consumer group ID for Kafka"
    )

    @model_validator(mode="after")
    def validate_lag_policy_pair(cls, values: "TopicConfig") -> "TopicConfig":
        if (values.lag_policy is None) ^ (values.acceptable_lag is None):
            raise ValueError("Both 'lag_policy' and 'acceptable_lag' must be set together (or both omitted).")
        if values.kafka_bootstrap_servers is None:
            raise ValueError("kafka bootstrap servers is mandatory")
        return values
