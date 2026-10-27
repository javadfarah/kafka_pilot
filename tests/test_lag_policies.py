"""Tests for Kafka lag calculation and lag policies."""
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from aiokafka import TopicPartition
from aiokafka.structs import OffsetAndTimestamp


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src") if (ROOT / "src").exists() else str(ROOT.parent))
os.environ.setdefault("KFP__REDIS_HOST", "localhost")
os.environ.setdefault("KFP__REDIS_PORT", "6379")
os.environ.setdefault("KFP__REDIS_DB_NUMBER", "0")
os.environ.setdefault(
    "KFP__KAFKA_CONF_DIRECTORY",
    str(ROOT / "tests" / "fixtures" / "processors"),
)

from kafka_pilot.lag_policies import (  # noqa: E402
    LagPolicyInterface,
    SeekToTheEndLagPolicy,
    StartOfDayLagPolicy,
)
from kafka_pilot.worker import KafkaWorker  # noqa: E402


class LagPolicyRegistryTests(unittest.TestCase):
    def test_policies_are_registered_by_class_name(self):
        self.assertIs(LagPolicyInterface.registry["StartOfDayLagPolicy"], StartOfDayLagPolicy)
        self.assertIs(LagPolicyInterface.registry["SeekToTheEndLagPolicy"], SeekToTheEndLagPolicy)


class GetLagPolicyClassTests(unittest.IsolatedAsyncioTestCase):
    def build_worker(self, lag_policy) -> KafkaWorker:
        processor = MagicMock()
        processor.topic_conf.lag_policy = lag_policy
        with patch("kafka_pilot.worker.get_conf", return_value=MagicMock()):
            return KafkaWorker(processor, "orders")

    async def test_none_lag_policy_is_a_no_op(self):
        worker = self.build_worker(None)
        await worker._get_lag_policy_class()
        self.assertIsNone(worker.lag_policy)

    async def test_invalid_lag_policy_raises(self):
        worker = self.build_worker("NotARealPolicy")
        with self.assertRaises(ValueError):
            await worker._get_lag_policy_class()

    async def test_valid_lag_policy_resolves_to_class(self):
        worker = self.build_worker("SeekToTheEndLagPolicy")
        await worker._get_lag_policy_class()
        self.assertIs(worker.lag_policy, SeekToTheEndLagPolicy)


class CheckLagTests(unittest.IsolatedAsyncioTestCase):
    def build_worker(self) -> KafkaWorker:
        processor = MagicMock()
        processor.topic_conf.lag_policy = "SeekToTheEndLagPolicy"
        processor.topic_conf.acceptable_lag = 1000
        with patch("kafka_pilot.worker.get_conf", return_value=MagicMock()):
            worker = KafkaWorker(processor, "orders")
        worker.lag_policy = SeekToTheEndLagPolicy
        worker.redis_client = MagicMock()
        worker.redis_client.set_topic_lag = AsyncMock()
        return worker

    async def test_missing_commit_uses_log_start_not_latest_position(self):
        worker = self.build_worker()
        tp = TopicPartition("orders", 0)
        consumer = MagicMock()
        consumer.assignment = MagicMock(return_value={tp})
        consumer.end_offsets = AsyncMock(side_effect=[{tp: 5000}, {tp: 5000}])
        consumer.beginning_offsets = AsyncMock(return_value={tp: 100})
        consumer.committed = AsyncMock(return_value=None)
        consumer.position = AsyncMock(return_value=5000)
        worker.consumer = consumer

        policy_instance = MagicMock()
        policy_instance.execute = AsyncMock()
        worker.lag_policy = MagicMock(return_value=policy_instance)

        await worker._check_lag_and_seek_if_needed()

        # Fresh group: lag = end - log_start = 4900, above acceptable_lag=1000
        worker.lag_policy.assert_called_once()
        self.assertEqual(worker.lag_policy.call_args.kwargs["committed_offsets"], {tp: 100})
        self.assertEqual(worker.redis_client.set_topic_lag.await_args_list[0].args[2], 4900)

    async def test_high_lag_runs_policy_and_refreshes_redis_lag(self):
        worker = self.build_worker()
        tp = TopicPartition("orders", 0)
        consumer = MagicMock()
        consumer.assignment = MagicMock(return_value={tp})
        consumer.end_offsets = AsyncMock(side_effect=[{tp: 5000}, {tp: 5000}])
        consumer.beginning_offsets = AsyncMock(return_value={tp: 0})
        consumer.committed = AsyncMock(return_value=100)
        consumer.position = AsyncMock(return_value=5000)
        worker.consumer = consumer

        policy_instance = MagicMock()
        policy_instance.execute = AsyncMock()
        worker.lag_policy = MagicMock(return_value=policy_instance)

        await worker._check_lag_and_seek_if_needed()

        worker.lag_policy.assert_called_once()
        policy_instance.execute.assert_awaited_once()
        # Pre-seek lag (4900) then post-seek refresh (0)
        self.assertEqual(
            [c.args[2] for c in worker.redis_client.set_topic_lag.await_args_list],
            [4900, 0],
        )


class StartOfDayLagPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_seek_and_commit_when_behind_today(self):
        tp = TopicPartition("orders", 0)
        consumer = MagicMock()
        consumer.offsets_for_times = AsyncMock(
            return_value={tp: OffsetAndTimestamp(offset=200, timestamp=1)}
        )
        consumer.seek = MagicMock()
        consumer.commit = AsyncMock()

        policy = StartOfDayLagPolicy(
            total_lag=10_000,
            partitions=[tp],
            consumer=consumer,
            committed_offsets={tp: 50},
        )
        await policy.execute()

        consumer.seek.assert_called_once_with(tp, 200)
        consumer.commit.assert_awaited_once_with({tp: 200})

    async def test_no_commit_when_already_at_or_past_today(self):
        tp = TopicPartition("orders", 0)
        consumer = MagicMock()
        consumer.offsets_for_times = AsyncMock(
            return_value={tp: OffsetAndTimestamp(offset=200, timestamp=1)}
        )
        consumer.seek = MagicMock()
        consumer.commit = AsyncMock()

        policy = StartOfDayLagPolicy(
            total_lag=10_000,
            partitions=[tp],
            consumer=consumer,
            committed_offsets={tp: 250},
        )
        await policy.execute()

        consumer.seek.assert_not_called()
        consumer.commit.assert_not_awaited()


class SeekToTheEndLagPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_seek_to_end_and_commit(self):
        tp = TopicPartition("orders", 0)
        consumer = MagicMock()
        consumer.seek_to_end = AsyncMock()
        consumer.end_offsets = AsyncMock(return_value={tp: 999})
        consumer.commit = AsyncMock()

        policy = SeekToTheEndLagPolicy(
            total_lag=10_000,
            partitions=[tp],
            consumer=consumer,
            committed_offsets={tp: 1},
        )
        await policy.execute()

        consumer.seek_to_end.assert_awaited_once_with(tp)
        consumer.commit.assert_awaited_once_with({tp: 999})


if __name__ == "__main__":
    unittest.main()
