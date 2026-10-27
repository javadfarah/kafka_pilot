"""Tests for worker status bookkeeping in Redis.

A worker that is gone must never keep advertising itself as running.
"""
import asyncio
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src") if (ROOT / "src").exists() else str(ROOT.parent))
os.environ.setdefault("KFP__REDIS_HOST", "localhost")
os.environ.setdefault("KFP__REDIS_PORT", "6379")
os.environ.setdefault("KFP__REDIS_DB_NUMBER", "0")
os.environ.setdefault(
    "KFP__KAFKA_CONF_DIRECTORY",
    str(ROOT / "tests" / "fixtures" / "processors"),
)

from kafka_pilot.db.redis import (  # noqa: E402
    TERMINAL_STATUS_TTL_SECONDS,
    WORKER_LEASE_SECONDS,
    RedisClient,
)
from kafka_pilot.utils import logger  # noqa: E402
from kafka_pilot.worker import KafkaWorker, decode_value  # noqa: E402


def build_redis_client() -> RedisClient:
    """A RedisClient wired to an AsyncMock connection."""
    client = RedisClient.__new__(RedisClient)
    client.redis = AsyncMock()
    client.config = MagicMock()
    return client


class WorkerStatusKeyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = build_redis_client()

    async def test_running_status_expires_so_a_killed_worker_stops_looking_alive(self):
        await self.client.set_worker_status("orders", "w1", "running", {0})

        self.client.redis.expire.assert_awaited_once_with(
            "worker:orders:w1", WORKER_LEASE_SECONDS
        )
        self.client.redis.persist.assert_not_awaited()

    async def test_terminal_status_is_retained_for_the_longer_window(self):
        await self.client.set_worker_status("orders", "w1", "stopped", set())

        self.client.redis.expire.assert_awaited_once_with(
            "worker:orders:w1", TERMINAL_STATUS_TTL_SECONDS
        )

    async def test_failed_status_is_also_terminal(self):
        await self.client.set_worker_status("orders", "w1", "failed", set())

        self.client.redis.expire.assert_awaited_once_with(
            "worker:orders:w1", TERMINAL_STATUS_TTL_SECONDS
        )

    async def test_mark_all_workers_stopped_clears_previous_run_records(self):
        self.client.redis.keys = AsyncMock(
            return_value=["worker:orders:old-1", "worker:orders:old-2"]
        )

        cleared = await self.client.mark_all_workers_stopped("orders")

        self.assertEqual(["old-1", "old-2"], cleared)
        statuses = [
            call.kwargs["mapping"]["status"]
            for call in self.client.redis.hset.await_args_list
        ]
        self.assertEqual(["stopped", "stopped"], statuses)


class WorkerLifecycleStatusTests(unittest.IsolatedAsyncioTestCase):
    def build_worker(self) -> KafkaWorker:
        processor = MagicMock()
        processor.topic_conf.lag_policy = None
        with patch("kafka_pilot.worker.get_conf", return_value=MagicMock()):
            worker = KafkaWorker(processor, "orders")
        worker.redis_client = MagicMock()
        worker.redis_client.set_worker_status = AsyncMock()
        return worker

    def recorded_statuses(self, worker) -> list:
        return [
            call.args[2] for call in worker.redis_client.set_worker_status.await_args_list
        ]

    async def test_stop_records_stopped_after_the_consumer_is_closed(self):
        worker = self.build_worker()
        worker.running = True
        order = []
        worker.consumer = MagicMock()
        worker.consumer.stop = AsyncMock(side_effect=lambda: order.append("consumer"))
        worker.redis_client.set_worker_status = AsyncMock(
            side_effect=lambda *a, **kw: order.append(a[2])
        )

        await worker.stop()

        self.assertFalse(worker.running)
        self.assertEqual(["consumer", "stopped"], order)

    async def test_stop_is_not_overwritten_by_a_racing_kafka_initialization(self):
        """A stop during startup must not be reverted to running."""
        worker = self.build_worker()
        worker.running = False
        worker.lag_policy = "SeekToTheEndLagPolicy"
        worker._check_lag_and_seek_if_needed = AsyncMock()
        consumer = MagicMock()
        consumer.start = AsyncMock()
        consumer.assignment = MagicMock(return_value=set())

        with patch("kafka_pilot.worker.AIOKafkaConsumer", return_value=consumer) as consumer_cls:
            await worker._initialize_kafka()

        self.assertIs(consumer_cls.call_args.kwargs["value_deserializer"], decode_value)
        self.assertEqual([], self.recorded_statuses(worker))

    async def test_worker_is_recorded_stopped_when_the_consume_loop_crashes(self):
        worker = self.build_worker()
        worker._initialize_kafka = AsyncMock(side_effect=RuntimeError("broker gone"))

        with self.assertRaises(RuntimeError):
            await worker.start()

        self.assertEqual(["running", "stopped"], self.recorded_statuses(worker))
        self.assertIsNone(worker._heartbeat_task)

    async def test_stopped_is_recorded_once_across_stop_and_loop_unwind(self):
        worker = self.build_worker()
        worker.running = True
        worker.consumer = MagicMock()
        worker.consumer.stop = AsyncMock()

        await worker.stop()
        await worker._record_stopped()

        self.assertEqual(["stopped"], self.recorded_statuses(worker))

    async def test_shutdown_closing_redis_after_stop_logs_no_error(self):
        """Shutdown stops workers, closes Redis, then the start() task unwinds."""
        worker = self.build_worker()
        worker._initialize_kafka = AsyncMock()

        async def consume_until_stopped():
            while worker.running:
                await asyncio.sleep(0)

        worker._consume_loop = AsyncMock(side_effect=consume_until_stopped)
        run_task = asyncio.create_task(worker.start())
        await asyncio.sleep(0)

        await worker.stop()
        # Redis is closed once every worker has been stopped.
        worker.redis_client.set_worker_status = AsyncMock(
            side_effect=RuntimeError("Redis client not initialized")
        )

        with self.assertLogs("json_logger", level="ERROR") as captured:
            logger.error("sentinel")
            await run_task

        self.assertEqual(
            ["sentinel"], [record.getMessage() for record in captured.records]
        )

    async def test_heartbeat_renews_the_lease_while_running(self):
        worker = self.build_worker()
        worker.running = True

        with patch("kafka_pilot.worker.WORKER_HEARTBEAT_SECONDS", 0):
            task = asyncio.create_task(worker._heartbeat_loop())
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            worker.running = False
            await task

        self.assertGreater(len(self.recorded_statuses(worker)), 0)
        self.assertEqual({"running"}, set(self.recorded_statuses(worker)))

    async def test_heartbeat_survives_a_redis_outage(self):
        worker = self.build_worker()
        worker.running = True
        worker.redis_client.set_worker_status = AsyncMock(
            side_effect=RuntimeError("redis down")
        )

        with patch("kafka_pilot.worker.WORKER_HEARTBEAT_SECONDS", 0):
            task = asyncio.create_task(worker._heartbeat_loop())
            await asyncio.sleep(0)
            worker.running = False
            await task

        self.assertTrue(task.done())
        self.assertIsNone(task.exception())


class ConsumeLoopAlwaysCommitTests(unittest.IsolatedAsyncioTestCase):
    def build_worker(self, process) -> KafkaWorker:
        processor = MagicMock()
        processor.topic_conf.lag_policy = None
        processor.topic_conf.auto_commit = False
        processor.topic_conf.max_retries = 0
        processor.topic_conf.processing_timeout_s = 0.05
        processor.topic_conf.backoff_ms = 1
        processor.topic_conf.backoff_multiplier = 1
        processor.process = process
        with patch("kafka_pilot.worker.get_conf", return_value=MagicMock()):
            worker = KafkaWorker(processor, "orders")
        worker.redis_client = MagicMock()
        worker.redis_client.set_worker_status = AsyncMock()
        worker.redis_client.increment_retry = AsyncMock()
        worker.redis_client.log_message = AsyncMock()
        worker.redis_client.increment_processed = AsyncMock()
        worker.running = True
        return worker

    def fake_consumer(self, worker, msg):
        class _Consumer:
            def __init__(self):
                self._yielded = False
                self.commit = AsyncMock()

            def __aiter__(self):
                return self

            async def __anext__(self):
                if self._yielded:
                    worker.running = False
                    raise StopAsyncIteration
                self._yielded = True
                return msg

        return _Consumer()

    def message(self):
        msg = MagicMock()
        msg.topic = "orders"
        msg.partition = 0
        msg.offset = 10
        return msg

    async def test_commit_happens_when_processor_hangs(self):
        async def hang(_msg):
            await asyncio.sleep(10)

        worker = self.build_worker(hang)
        msg = self.message()
        worker.consumer = self.fake_consumer(worker, msg)

        await worker._consume_loop()

        worker.consumer.commit.assert_awaited()
        committed = worker.consumer.commit.await_args.args[0]
        self.assertEqual(next(iter(committed.values())), msg.offset + 1)

    async def test_commit_happens_when_processor_raises(self):
        async def boom(_msg):
            raise RuntimeError("processor exploded")

        worker = self.build_worker(boom)
        msg = self.message()
        worker.consumer = self.fake_consumer(worker, msg)

        await worker._consume_loop()

        worker.consumer.commit.assert_awaited()


class DecodeValueTests(unittest.TestCase):
    def test_none_and_empty_return_none(self):
        self.assertIsNone(decode_value(None))
        self.assertIsNone(decode_value(b""))

    def test_parses_json_bytes(self):
        self.assertEqual({"a": 1}, decode_value(b'{"a":1}'))


if __name__ == "__main__":
    unittest.main()
