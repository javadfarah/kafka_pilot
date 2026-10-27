from abc import ABC, abstractmethod
from typing import Any, Dict


class MessagePublisher(ABC):
    """Abstract base class for publishing messages to different backends.

    Subclasses implement broker or storage-specific logic
    (Kafka, Redis, RabbitMQ, databases, etc.).

    Attributes:
        registry (Dict): A registry mapping class names to publisher subclasses.
        enabled (bool): Whether the publisher is active and should be registered.
    """
    registry: Dict = {}

    def __init__(self, **kwargs):
        """Initialize the publisher with optional configuration."""
        self.config = kwargs
        self._is_ready = False

    @abstractmethod
    async def prepare_data(self, key: Any, message: Any) -> Any:
        """Transform/serialize data before publishing."""
        pass

    @abstractmethod
    async def publish(self, key, message, **kwargs):
        """Publish a single message to the backend."""
        pass
