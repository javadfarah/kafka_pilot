from datetime import datetime, timezone

from kafka_pilot.lag_policies.lag_policy_interface import LagPolicyInterface
from kafka_pilot.utils import logger


class StartOfDayLagPolicy(LagPolicyInterface):
    """Lag policy that seeks Kafka partitions to today's 00:00 UTC if lag is high.

    This policy checks the total lag across partitions, and if it exceeds a threshold,
    it seeks each partition to the offset corresponding to today's 00:00 UTC. This
    ensures consumers start processing messages from the beginning of the current day,
    preventing processing of excessive backlog.

    Attributes:
        total_lag (int): Total lag across all partitions.
        partitions (List[Partition]): List of Kafka partitions being consumed.
        committed_offsets (Dict[Partition, int]): Last committed offsets per partition.
        consumer (AIOKafkaConsumer): The Kafka consumer instance.
    """

    async def execute(self):

        """Execute the Start-of-Day lag policy.

        If the total lag is below the threshold (200,000 messages), no action is taken.
        Otherwise, this method calculates the offsets corresponding to 00:00 UTC today
        for each partition and seeks the consumer to those offsets if necessary.

        Logs all actions, including warnings for high lag or missing offsets, and info
        messages for seek operations.

        Returns:
            None
        """

        # Get offset for today 00:00 UTC
        today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        timestamp_ms = int(today_utc.timestamp() * 1000)
        timestamps = {tp: timestamp_ms for tp in self.partitions}

        offsets_for_time = await self.consumer.offsets_for_times(timestamps)

        for tp in self.partitions:
            offset_info = offsets_for_time.get(tp)
            committed_offset = self.committed_offsets.get(tp, 0)

            if offset_info is None:
                logger.warning(f"No offset found at or after today's timestamp for {tp}. Skipping seek.")
                continue

            today_offset = offset_info.offset

            if committed_offset < today_offset:
                self.consumer.seek(tp, today_offset)
                logger.info(
                    f"Seeked partition {tp.partition} to offset {today_offset} (committed was {committed_offset})"
                )
            else:
                logger.info(
                    f"No seek needed for partition {tp.partition} (committed: {committed_offset}, today offset: {today_offset})"
                )
