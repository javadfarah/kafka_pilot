from kafka_pilot.lag_policies.lag_policy_interface import LagPolicyInterface


class SeekToTheEndLagPolicy(LagPolicyInterface):
    """Lag policy that seeks Kafka partitions to the end (latest offset).

    This policy moves all specified partitions to their latest offsets,
    effectively skipping any pending backlog. It is useful when a consumer
    should start processing only new incoming messages and ignore old ones.

    Attributes:
        partitions (List[Partition]): List of Kafka partitions being consumed.
        consumer (AIOKafkaConsumer): The Kafka consumer instance.
    """

    async def execute(self):
        """Execute the Seek-to-End lag policy.

         Seeks all partitions to the latest offset. No checks or conditions are applied.

         Returns:
             None
         """
        await self.consumer.seek_to_end()
