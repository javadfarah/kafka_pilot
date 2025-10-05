import asyncio
from abc import ABC, abstractmethod
from typing import Dict

from aiokafka.structs import ConsumerRecord

from kafka_pilot.schema.topic_conf import TopicConfig
from kafka_pilot.utils import logger


class MessageProcessor(ABC):
    """Abstract base class for processing Kafka messages.

    This class defines a framework for message processors that consume and
    process Kafka messages based on configurable topic settings. Subclasses
    must implement the `topic_conf` property and the `process` method.

    Attributes:
        registry (Dict): A registry mapping class names to processor subclasses.
        enabled (bool): Whether the processor is active and should be registered.
        _should_send_to_dlq (bool): Internal flag to indicate DLQ redirection.
        batch_count (int): Number of messages processed in a batch.
        processed_data (dict): Stores processed message data (customizable).
    """
    registry: Dict = {}
    enabled: bool = True
    instances: int = 1

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if getattr(cls, "enabled", True):
            MessageProcessor.registry[cls.__name__] = cls

    @property
    @abstractmethod
    def topic_conf(self) -> TopicConfig:
        """A required property."""
        pass

    def __init__(self, batch_count=200, *args, **kwargs):
        """Initialize processor with topic configuration."""
        self._should_send_to_dlq = False
        self.batch_count = batch_count
        self.processed_data = {}

    @abstractmethod
    async def process(self, message: ConsumerRecord):
        """Process a single Kafka message.

        Subclasses must override this method with custom business logic.
        Example implementation shows logging and error handling.

        Args:
            message (ConsumerRecord): A Kafka message to process.

        Raises:
            Exception: If message processing fails.
        """
        try:
            # Example processing logic (replace with actual business logic)
            logger.info(f"Processing message {message.value}")
            # Simulate processing
            await asyncio.sleep(0.1)
            # Example condition for DLQ
            if b"error" in message.value.lower():
                self._should_send_to_dlq = True
        except Exception as e:
            logger.error(f"Processing error: {e}")
            raise

    def should_send_to_dlq(self) -> bool:
        """Check if message should be sent to DLQ."""
        return self._should_send_to_dlq
