from kafka_pilot.lag_policies.lag_policy_interface import LagPolicyInterface
from kafka_pilot.utils import logger


class SeekToTheEndLagPolicy(LagPolicyInterface):
    """Lag policy that seeks Kafka partitions to the end (latest offset).

    Moves assigned partitions to their high watermarks and commits those offsets
    so skipped backlog does not reappear after rebalance or restart.
    """

    async def execute(self):
        """Seek assigned partitions to end and commit the new positions."""
        partitions = list(self.partitions) if self.partitions else None
        if partitions:
            await self.consumer.seek_to_end(*partitions)
            end_offsets = await self.consumer.end_offsets(partitions)
        else:
            await self.consumer.seek_to_end()
            assigned = list(self.consumer.assignment())
            end_offsets = await self.consumer.end_offsets(assigned) if assigned else {}

        if end_offsets:
            await self.consumer.commit(end_offsets)
            logger.info(f"Seeked to end and committed offsets for {len(end_offsets)} partition(s)")
