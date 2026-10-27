from abc import ABC, abstractmethod
from typing import List, Dict

from aiokafka import TopicPartition, AIOKafkaConsumer


class LagPolicyInterface(ABC):
    """Abstract base class for Kafka consumer lag policies.

    This interface defines a framework for implementing different lag handling
    strategies for Kafka consumers. Subclasses must implement the `execute`
    method, which applies the policy logic.

    Attributes:
        registry (Dict): A registry mapping policy class names to their subclasses.
        total_lag (int): Total lag across all assigned partitions.
        partitions (List[TopicPartition]): List of partitions this policy applies to.
        consumer (AIOKafkaConsumer): Kafka consumer instance.
        committed_offsets (Dict[TopicPartition, int]): Last committed offsets per partition.
    """

    registry: Dict = {}

    def __init_subclass__(cls, **kwargs):
        """Automatically register all subclasses in the policy registry.

        Args:
            **kwargs: Additional keyword arguments for subclass initialization.
        """
        super().__init_subclass__(**kwargs)
        LagPolicyInterface.registry[cls.__name__] = cls

    def __init__(self, total_lag: int, partitions: List[TopicPartition], consumer: AIOKafkaConsumer,
                 committed_offsets: Dict):
        """Initialize a lag policy instance.

        Args:
            total_lag (int): Total lag across all partitions.
            partitions (List[TopicPartition]): Partitions to which this policy applies.
            consumer (AIOKafkaConsumer): Kafka consumer instance.
            committed_offsets (Dict[TopicPartition, int]): Last committed offsets per partition.
        """
        self.total_lag = total_lag
        self.partitions = partitions
        self.consumer = consumer
        self.committed_offsets = committed_offsets

    @abstractmethod
    async def execute(self):
        """Apply the lag policy.

        Subclasses must override this method to implement custom lag-handling logic.

        Raises:
            NotImplementedError: If not implemented in a subclass.
        """
        pass
