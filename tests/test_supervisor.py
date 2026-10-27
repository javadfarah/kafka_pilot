"""Tests for WorkerSupervisor monitoring, detection, and auto-healing."""
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

from kafka_pilot.supervisor import WorkerSupervisor, SupervisedWorker  # noqa: E402
from kafka_pilot.worker import KafkaWorker  # noqa: E402


class WorkerSupervisorTests(unittest.IsolatedAsyncioTestCase):
    def build_worker(self, topic: str = "test-topic", worker_id: str = "w1") -> KafkaWorker:
        processor = MagicMock()
        processor.topic_conf.lag_policy = None
        with patch("kafka_pilot.worker.get_conf", return_value=MagicMock()):
            worker = KafkaWorker(processor, topic)
        worker.worker_id = worker_id
        worker.redis_client = MagicMock()
        worker.redis_client.set_worker_status = AsyncMock()
        worker.running = True
        return worker

    async def test_supervisor_detects_healthy_workers(self):
        supervisor = WorkerSupervisor(check_interval_seconds=1.0, startup_grace_period_seconds=0.0)
        worker = self.build_worker()

        async def dummy_run():
            while worker.running:
                await asyncio.sleep(0.01)

        task = asyncio.create_task(dummy_run())
        supervisor.register_worker(worker, task)

        health = supervisor.get_health_status()
        self.assertEqual(health["status"], "healthy")
        self.assertEqual(health["total_workers"], 1)
        self.assertEqual(health["active_workers"], 1)
        self.assertEqual(health["failed_workers"], 0)

        worker.running = False
        await task

    async def test_supervisor_detects_dead_worker_and_restarts_it(self):
        supervisor = WorkerSupervisor(
            check_interval_seconds=0.05,
            startup_grace_period_seconds=0.0,
            apply_backoff=False,
        )
        worker = self.build_worker()
        worker.start = AsyncMock()

        async def crashing_run():
            raise RuntimeError("simulated crash")

        task = asyncio.create_task(crashing_run())
        await asyncio.sleep(0.01)  # Let it crash
        supervisor.register_worker(worker, task)

        # Run one supervision tick
        await supervisor.check_workers()
        await asyncio.sleep(0.01)  # Allow restart task to spawn

        health = supervisor.get_health_status()
        self.assertEqual(health["total_workers"], 1)
        worker.start.assert_called_once()
        supervised = supervisor.get_supervised_worker(worker.worker_id)
        self.assertIsNotNone(supervised)
        self.assertEqual(supervised.restart_count, 1)
        self.assertIn("simulated crash", str(supervised.last_error))

    async def test_supervisor_marks_failed_after_max_retries(self):
        supervisor = WorkerSupervisor(check_interval_seconds=0.05, startup_grace_period_seconds=0.0)
        worker = self.build_worker()
        worker.start = AsyncMock()

        async def dead_task():
            pass

        task = asyncio.create_task(dead_task())
        await task

        supervisor.register_worker(worker, task)
        supervised = supervisor.get_supervised_worker(worker.worker_id)
        supervised.restart_count = 5  # Already reached 5 retries

        await supervisor.check_workers()

        self.assertEqual(supervised.state, "failed")
        health = supervisor.get_health_status()
        self.assertEqual(health["status"], "unhealthy")
        self.assertEqual(health["failed_workers"], 1)

    async def test_supervisor_does_not_restart_intentionally_stopped_worker(self):
        supervisor = WorkerSupervisor(check_interval_seconds=0.05, startup_grace_period_seconds=0.0)
        worker = self.build_worker()
        worker.running = False  # Intentionally stopped
        worker.start = AsyncMock()

        async def clean_exit():
            pass

        task = asyncio.create_task(clean_exit())
        await task
        supervisor.register_worker(worker, task)

        await supervisor.check_workers()

        # Should not restart because worker.running is False
        worker.start.assert_not_called()
        health = supervisor.get_health_status()
        self.assertEqual(health["failed_workers"], 0)

    async def test_supervisor_lifecycle_start_and_stop(self):
        supervisor = WorkerSupervisor(check_interval_seconds=0.05, startup_grace_period_seconds=0.0)
        worker = self.build_worker()

        async def run_loop():
            while worker.running:
                await asyncio.sleep(0.01)

        task = asyncio.create_task(run_loop())
        supervisor.register_worker(worker, task)

        await supervisor.start()
        self.assertTrue(supervisor.is_running())

        await supervisor.stop()
        self.assertFalse(supervisor.is_running())

        worker.running = False
        await task


class HealthCheckEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_health_check_returns_200_when_healthy(self):
        from kafka_pilot.main import health_check
        from kafka_pilot.db import redis_client
        from kafka_pilot.supervisor import worker_supervisor

        worker_supervisor._workers.clear()
        mock_redis = MagicMock()
        mock_redis.ping = AsyncMock(return_value=True)
        with patch.object(redis_client, "redis", mock_redis):
            res = await health_check()
            self.assertEqual(res.status_code, 200)

    async def test_health_check_returns_503_when_redis_down(self):
        from kafka_pilot.main import health_check
        from kafka_pilot.db import redis_client
        from kafka_pilot.supervisor import worker_supervisor

        worker_supervisor._workers.clear()
        mock_redis = MagicMock()
        mock_redis.ping = AsyncMock(side_effect=RuntimeError("connection refused"))
        with patch.object(redis_client, "redis", mock_redis):
            res = await health_check()
            self.assertEqual(res.status_code, 503)

    async def test_health_check_returns_503_when_worker_failed(self):
        from kafka_pilot.main import health_check
        from kafka_pilot.db import redis_client
        from kafka_pilot.supervisor import worker_supervisor, SupervisedWorker

        mock_redis = MagicMock()
        mock_redis.ping = AsyncMock(return_value=True)
        with patch.object(redis_client, "redis", mock_redis):
            worker_supervisor._workers["failed-worker"] = SupervisedWorker(
                topic="orders",
                worker_id="failed-worker",
                worker=MagicMock(),
                task=None,
                state="failed",
                last_error="socket closed",
            )
            res = await health_check()
            self.assertEqual(res.status_code, 503)
            worker_supervisor._workers.clear()

    async def test_health_check_returns_200_degraded_when_worker_restarting(self):
        from kafka_pilot.main import health_check
        from kafka_pilot.db import redis_client
        from kafka_pilot.supervisor import worker_supervisor, SupervisedWorker

        mock_redis = MagicMock()
        mock_redis.ping = AsyncMock(return_value=True)
        with patch.object(redis_client, "redis", mock_redis):
            worker_supervisor._workers["restarting-worker"] = SupervisedWorker(
                topic="orders",
                worker_id="restarting-worker",
                worker=MagicMock(),
                task=None,
                state="restarting",
                restart_count=1,
                last_error="socket reset",
            )
            res = await health_check()
            self.assertEqual(res.status_code, 200)
            worker_supervisor._workers.clear()

    async def test_health_check_surfaces_kafka_clue_when_worker_has_kafka_errors(self):
        import json
        from kafka_pilot.main import health_check
        from kafka_pilot.db import redis_client
        from kafka_pilot.supervisor import worker_supervisor, SupervisedWorker

        mock_redis = MagicMock()
        mock_redis.ping = AsyncMock(return_value=True)
        mock_worker = MagicMock()
        mock_worker.last_error = "KafkaConnectionError: Connection at 10.104.137.71:9094 closed"
        mock_worker.consecutive_errors = 4
        mock_worker.last_error_at = None

        running_task = asyncio.create_task(asyncio.sleep(10))
        try:
            with patch.object(redis_client, "redis", mock_redis):
                worker_supervisor._workers["err-worker"] = SupervisedWorker(
                    topic="market.trades",
                    worker_id="err-worker",
                    worker=mock_worker,
                    task=running_task,
                    state="running",
                )
                await worker_supervisor.check_workers()
                res = await health_check()
                data = json.loads(bytes(res.body).decode())

                self.assertEqual(res.status_code, 200)
                self.assertEqual(data["status"], "degraded")
                self.assertEqual(data["kafka"]["status"], "degraded")
                self.assertEqual(len(data["kafka"]["errors"]), 1)
                self.assertEqual(
                    data["kafka"]["errors"][0]["error"],
                    "KafkaConnectionError: Connection at 10.104.137.71:9094 closed",
                )
                self.assertEqual(data["kafka"]["errors"][0]["consecutive_errors"], 4)
                worker_supervisor._workers.clear()
        finally:
            running_task.cancel()
            try:
                await running_task
            except asyncio.CancelledError:
                pass


if __name__ == "__main__":
    unittest.main()
