from datetime import datetime, timezone

from kafka_pilot.lag_policies.lag_policy_interface import LagPolicyInterface
from kafka_pilot.utils import logger


class StartOfDayLagPolicy(LagPolicyInterface):
    """Lag policy that seeks Kafka partitions to today's 00:00 UTC if lag is high.

    When lag exceeds the worker's configured ``acceptable_lag``, each partition is
    sought to the first offset at or after today's 00:00 UTC. New positions are
    committed so the skip survives rebalance/restart.
    """

    async def execute(self):
        """Seek partitions behind today's UTC midnight offset and commit them."""

        today_utc = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        timestamp_ms = int(today_utc.timestamp() * 1000)
        timestamps = {tp: timestamp_ms for tp in self.partitions}

        offsets_for_time = await self.consumer.offsets_for_times(timestamps)
        to_commit = {}

        for tp in self.partitions:
            offset_info = offsets_for_time.get(tp)
            committed_offset = self.committed_offsets.get(tp, 0)

            if offset_info is None or offset_info.offset < 0:
                logger.warning(
                    f"No offset found at or after today's timestamp for {tp}. Not seeking."
                )
                continue

            today_offset = offset_info.offset

            if committed_offset < today_offset:
                self.consumer.seek(tp, today_offset)
                # Commit the next offset to consume (= seek target).
                to_commit[tp] = today_offset
                logger.info(
                    f"Seeked partition {tp.partition} to offset {today_offset} (committed was {committed_offset})"
                )
            else:
                logger.info(
                    f"No seek needed for partition {tp.partition} (committed: {committed_offset}, today offset: {today_offset})"
                )

        if to_commit:
            await self.consumer.commit(to_commit)
            logger.info(f"Committed start-of-day seeks for {len(to_commit)} partition(s)")
