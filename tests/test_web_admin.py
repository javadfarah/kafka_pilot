"""Integration tests for Kafka Pilot's admin API and dashboard."""
import os
import sys
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src") if (ROOT / "src").exists() else str(ROOT.parent))
os.environ.setdefault("KFP__REDIS_HOST", "localhost")
os.environ.setdefault("KFP__REDIS_PORT", "6379")
os.environ.setdefault("KFP__REDIS_DB_NUMBER", "0")
os.environ.setdefault(
    "KFP__KAFKA_CONF_DIRECTORY",
    str(ROOT / "tests" / "fixtures" / "processors"),
)

from kafka_pilot import web_admin  # noqa: E402


class FakeProcessor:
    """Minimal processor class used by worker creation routes."""


class FakeWorker:
    def __init__(
        self,
        worker_id: str = "memory-worker",
        *,
        running: bool = True,
        partitions=None,
    ):
        self.worker_id = worker_id
        self.running = running
        self.partitions = set(partitions or [])
        self.stop = AsyncMock(side_effect=self._mark_stopped)
        self.start = AsyncMock(side_effect=self._mark_started)

    async def _mark_stopped(self):
        self.running = False

    async def _mark_started(self):
        self.running = True


class AdminPanelTests(unittest.TestCase):
    def setUp(self):
        self.worker = FakeWorker(partitions={2, 0})
        self.workers = {"orders": [self.worker]}
        self.registry = {
            "orders": {
                "processor_class": FakeProcessor,
                "processor_name": "FakeProcessor",
                "instances": 2,
            }
        }

        self.stack = ExitStack()
        self.stack.enter_context(
            patch.object(web_admin, "_build_topic_registry", return_value=self.registry)
        )
        redis = web_admin.redis_client
        redis.redis = AsyncMock()
        redis.redis.ping = AsyncMock(return_value=True)
        self.stack.enter_context(
            patch.object(
                redis,
                "get_topic_lags",
                AsyncMock(
                    return_value={
                        "memory-worker": {"lag": 4, "updated_at": "2026-08-01T08:00:00Z"},
                        "orphan-worker": {"lag": 7, "updated_at": "2026-08-01T08:01:00Z"},
                    }
                ),
            )
        )
        self.stack.enter_context(
            patch.object(
                redis,
                "get_all_worker_details",
                AsyncMock(
                    return_value={
                        "memory-worker": {
                            "status": "running",
                            "partitions": [0, 2],
                            "updated_at": "2026-08-01T08:00:00Z",
                        },
                        "orphan-worker": {
                            "status": "stopped",
                            "partitions": [1],
                            "updated_at": "2026-08-01T07:00:00Z",
                        },
                    }
                ),
            )
        )
        self.stack.enter_context(
            patch.object(
                redis,
                "get_metrics",
                AsyncMock(
                    return_value={
                        "processed": 1200,
                        "retries": 3,
                        "errors": 1,
                        "dlq": 2,
                    }
                ),
            )
        )
        self.stack.enter_context(
            patch.object(
                redis,
                "get_logs",
                AsyncMock(return_value=["processed offset 42", "Error: invalid payload"]),
            )
        )
        self.stack.enter_context(
            patch.object(redis, "get_log_count", AsyncMock(return_value=2))
        )
        self.stack.enter_context(
            patch.object(redis, "clear_logs", AsyncMock(return_value=None))
        )
        self.stack.enter_context(
            patch.object(redis, "reset_metrics", AsyncMock(return_value=None))
        )

        self.spawn_counter = 0
        self.stack.enter_context(
            patch.object(web_admin, "KafkaWorker", side_effect=self._make_worker)
        )

        app = FastAPI()
        web_admin.setup_web_admin(app, self.workers)
        self.client = TestClient(app)

    def _make_worker(self, processor, topic):
        self.spawn_counter += 1
        return FakeWorker(f"spawned-{self.spawn_counter}", running=False)

    def tearDown(self):
        self.client.close()
        web_admin.redis_client.redis = None
        self.stack.close()

    def test_dashboard_supports_material_themes_and_any_mount_path(self):
        response = self.client.get("/admin")

        self.assertEqual(200, response.status_code)
        self.assertIn("Material+Symbols+Rounded", response.text)
        self.assertIn('id="themeBtn"', response.text)
        self.assertIn('data-theme="dark"', response.text)
        self.assertIn('src="static/kafka_pilot.png"', response.text)
        logo = self.client.get("/static/kafka_pilot.png")
        self.assertEqual(200, logo.status_code)
        self.assertEqual("image/png", logo.headers["content-type"])
        self.assertIn(
            'const APP_BASE = window.location.pathname.replace(/\\/admin\\/?$/, "");',
            response.text,
        )
        self.assertNotIn("/kafka_pilot/api/", response.text)

        parent = FastAPI()
        child = FastAPI()
        web_admin.setup_web_admin(child, self.workers)
        parent.mount("/kafka_pilot", child)
        with TestClient(parent) as mounted_client:
            self.assertEqual(200, mounted_client.get("/kafka_pilot/admin").status_code)
            self.assertEqual(
                200,
                mounted_client.get("/kafka_pilot/static/kafka_pilot.png").status_code,
            )

    def test_overview_merges_workers_metrics_and_lag(self):
        response = self.client.get("/api/admin/overview")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("healthy", payload["status"])
        self.assertEqual(1, payload["totals"]["running"])
        self.assertEqual(1, payload["totals"]["stopped"])
        self.assertEqual(4, payload["totals"]["lag"])
        self.assertEqual(1200, payload["totals"]["processed"])
        self.assertEqual(["memory", "redis"], [
            worker["source"] for worker in payload["topics"][0]["workers"]
        ])
        self.assertEqual([0, 2], payload["topics"][0]["workers"][0]["partitions"])

    def test_worker_stop_and_missing_worker_responses(self):
        stopped = self.client.post(
            "/api/admin/topics/orders/workers/memory-worker/stop"
        )
        missing = self.client.post(
            "/api/admin/topics/orders/workers/does-not-exist/stop"
        )

        self.assertEqual(200, stopped.status_code)
        self.assertEqual("stopped", stopped.json()["status"])
        self.worker.stop.assert_awaited_once()
        self.assertEqual(404, missing.status_code)

    def test_logs_limit_validation_read_and_clear(self):
        invalid = self.client.get(
            "/api/admin/topics/orders/workers/memory-worker/logs?limit=1001"
        )
        logs = self.client.get(
            "/api/admin/topics/orders/workers/memory-worker/logs?limit=50"
        )
        cleared = self.client.delete(
            "/api/admin/topics/orders/workers/memory-worker/logs"
        )

        self.assertEqual(422, invalid.status_code)
        self.assertEqual(200, logs.status_code)
        self.assertEqual(2, logs.json()["total"])
        self.assertEqual("cleared", cleared.json()["status"])
        web_admin.redis_client.get_logs.assert_awaited_once_with(
            "orders", "memory-worker", limit=50, offset=0
        )
        web_admin.redis_client.clear_logs.assert_awaited_once_with(
            "orders", "memory-worker"
        )

    def test_topic_header_actions_reach_the_click_handler(self):
        """Header buttons must not be shadowed by the card-collapse handler."""
        page = self.client.get("/admin").text

        self.assertIn('<div class="topic-actions">', page)
        self.assertNotIn('onclick="event.stopPropagation()"', page)
        self.assertLess(
            page.index('const btn = e.target.closest("[data-action]");'),
            page.index('const toggle = e.target.closest("[data-toggle]");'),
        )

    def test_spawn_appends_a_new_worker_to_the_topic(self):
        response = self.client.post("/api/admin/topics/orders/workers/spawn")

        self.assertEqual(200, response.status_code)
        self.assertEqual("started", response.json()["status"])
        self.assertEqual("spawned-1", response.json()["worker_id"])
        self.assertEqual(
            ["memory-worker", "spawned-1"],
            [w.worker_id for w in self.workers["orders"]],
        )

    def test_spawn_rejects_unregistered_topic(self):
        response = self.client.post("/api/admin/topics/unknown/workers/spawn")

        self.assertEqual(404, response.status_code)

    def test_start_all_restarts_stopped_workers_and_skips_running_ones(self):
        self.workers["orders"].append(FakeWorker("idle-worker", running=False))

        response = self.client.post("/api/admin/topics/orders/workers/start-all")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual(["spawned-1"], payload["started"])
        self.assertEqual(["memory-worker"], payload["already_running"])
        self.assertEqual(
            ["memory-worker", "spawned-1"],
            [w.worker_id for w in self.workers["orders"]],
        )

    def test_start_all_spawns_configured_count_when_no_workers_exist(self):
        self.workers["orders"] = []

        response = self.client.post("/api/admin/topics/orders/workers/start-all")

        self.assertEqual(200, response.status_code)
        self.assertEqual(["spawned-1", "spawned-2"], response.json()["started"])
        self.assertEqual(2, len(self.workers["orders"]))

    def test_stop_all_stops_running_workers_and_reports_idle_ones(self):
        idle = FakeWorker("idle-worker", running=False)
        self.workers["orders"].append(idle)

        response = self.client.post("/api/admin/topics/orders/workers/stop-all")

        self.assertEqual(200, response.status_code)
        payload = response.json()
        self.assertEqual("ok", payload["status"])
        self.assertEqual(["memory-worker"], payload["stopped"])
        self.assertEqual(["idle-worker"], payload["already_stopped"])
        self.assertEqual([], payload["errors"])
        self.worker.stop.assert_awaited_once()
        idle.stop.assert_not_awaited()

    def test_stop_all_reports_partial_failure_without_aborting(self):
        failing = FakeWorker("broken-worker")
        failing.stop = AsyncMock(side_effect=RuntimeError("kafka gone"))
        self.workers["orders"].append(failing)

        response = self.client.post("/api/admin/topics/orders/workers/stop-all")

        payload = response.json()
        self.assertEqual("partial", payload["status"])
        self.assertEqual(["memory-worker"], payload["stopped"])
        self.assertEqual(
            [{"worker_id": "broken-worker", "error": "kafka gone"}], payload["errors"]
        )

    def test_stop_all_returns_404_when_topic_has_no_workers(self):
        self.workers["orders"] = []

        response = self.client.post("/api/admin/topics/orders/workers/stop-all")

        self.assertEqual(404, response.status_code)

    def test_reset_metrics_clears_counters_for_topic(self):
        response = self.client.post("/api/admin/topics/orders/metrics/reset")

        self.assertEqual(200, response.status_code)
        self.assertEqual("reset", response.json()["status"])
        web_admin.redis_client.reset_metrics.assert_awaited_once_with("orders")

    def test_reset_metrics_maps_redis_failure_to_service_unavailable(self):
        web_admin.redis_client.reset_metrics.side_effect = RuntimeError("offline")

        response = self.client.post("/api/admin/topics/orders/metrics/reset")

        self.assertEqual(503, response.status_code)
        self.assertEqual("offline", response.json()["detail"])


if __name__ == "__main__":
    unittest.main()
