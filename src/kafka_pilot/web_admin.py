"""Kafka Pilot web admin API and dashboard routes."""
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from fastapi import FastAPI, HTTPException, Query
from fastapi.requests import Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from kafka_pilot.db import redis_client
from kafka_pilot.supervisor import worker_supervisor
from kafka_pilot.utils import logger
from kafka_pilot.worker import KafkaWorker

TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
STATIC_DIR = TEMPLATES_DIR / "static"


class SeekToTimeRequest(BaseModel):
    datetime_str: str = Field(
        ...,
        description="ISO 8601 formatted datetime (e.g. 2026-08-25T10:00:00)",
    )
    timezone_str: Optional[str] = Field(
        default=None,
        description="Timezone name, e.g. Asia/Tehran, UTC",
    )
    reset_state: bool = Field(
        default=True,
        description="Whether to clear processor in-memory cursors & caches",
    )


def parse_target_datetime(datetime_str: str, timezone_str: Optional[str] = None) -> datetime:
    text = str(datetime_str).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid ISO datetime format: {datetime_str}")

    if dt.tzinfo is None:
        tz_name = timezone_str or "Asia/Tehran"
        try:
            tz = ZoneInfo(tz_name)
        except Exception:
            tz = timezone.utc
        dt = dt.replace(tzinfo=tz)
    elif timezone_str:
        try:
            tz = ZoneInfo(timezone_str)
            dt = dt.astimezone(tz)
        except Exception:
            pass

    return dt


def setup_web_admin(app: FastAPI, workers: Dict[str, List[KafkaWorker]]):
    """Set up web admin panel routes.

    ``workers`` maps topic -> list of KafkaWorker instances (multi-worker safe).
    """
    from kafka_pilot.processors import MessageProcessor

    topic_registry = _build_topic_registry()
    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
    app.mount(
        "/static",
        StaticFiles(directory=str(STATIC_DIR)),
        name="kafka-pilot-static",
    )

    def _processor_class_for_topic(topic: str):
        return topic_registry.get(topic, {}).get("processor_class")

    def _find_worker(topic: str, worker_id: str) -> Tuple[Optional[int], Optional[KafkaWorker]]:
        worker_list = workers.get(topic) or []
        for idx, worker in enumerate(worker_list):
            if worker.worker_id == worker_id:
                return idx, worker
        return None, None

    def _ensure_topic_list(topic: str) -> List[KafkaWorker]:
        if topic not in workers:
            workers[topic] = []
        return workers[topic]

    async def _serialize_in_memory_workers(topic: str) -> List[dict]:
        result = []
        for worker in workers.get(topic) or []:
            result.append({
                "worker_id": worker.worker_id,
                "topic": topic,
                "status": "running" if worker.running else "stopped",
                "partitions": sorted(worker.partitions) if worker.partitions else [],
                "source": "memory",
            })
        return result

    async def _merge_topic_workers(topic: str) -> List[dict]:
        """Merge in-memory workers with Redis registry (stopped orphans included)."""
        memory_workers = await _serialize_in_memory_workers(topic)
        memory_ids = {w["worker_id"] for w in memory_workers}

        lags = {}
        try:
            lags = await redis_client.get_topic_lags(topic)
        except RuntimeError:
            lags = {}

        redis_workers = []
        try:
            details = await redis_client.get_all_worker_details(topic)
            for worker_id, data in details.items():
                if worker_id in memory_ids:
                    # Enrich memory entry with Redis updated_at when present
                    for mw in memory_workers:
                        if mw["worker_id"] == worker_id:
                            mw["updated_at"] = data.get("updated_at")
                            # Prefer live partitions from memory; fall back to Redis
                            if not mw["partitions"] and data.get("partitions"):
                                mw["partitions"] = data["partitions"]
                            lag_info = lags.get(worker_id)
                            if lag_info:
                                mw["lag"] = lag_info.get("lag")
                                mw["lag_updated_at"] = lag_info.get("updated_at")
                            break
                    continue
                entry = {
                    "worker_id": worker_id,
                    "topic": topic,
                    "status": data.get("status", "unknown"),
                    "partitions": data.get("partitions") or [],
                    "updated_at": data.get("updated_at"),
                    "source": "redis",
                }
                lag_info = lags.get(worker_id)
                if lag_info:
                    entry["lag"] = lag_info.get("lag")
                    entry["lag_updated_at"] = lag_info.get("updated_at")
                redis_workers.append(entry)
        except RuntimeError as e:
            logger.error(f"Redis unavailable while listing workers for {topic}: {e}")

        # Attach lag for memory-only workers not yet in Redis details loop
        for mw in memory_workers:
            if "lag" not in mw and mw["worker_id"] in lags:
                mw["lag"] = lags[mw["worker_id"]].get("lag")
                mw["lag_updated_at"] = lags[mw["worker_id"]].get("updated_at")

        return memory_workers + redis_workers

    async def _restart_worker_at(topic: str, idx: int) -> KafkaWorker:
        processor_cls = _processor_class_for_topic(topic)
        if not processor_cls:
            raise HTTPException(status_code=404, detail=f"Topic {topic} not found in processor registry")
        new_worker = KafkaWorker(processor_cls(), topic)
        workers[topic][idx] = new_worker
        asyncio.create_task(new_worker.start())
        return new_worker

    async def _spawn_worker(topic: str) -> KafkaWorker:
        processor_cls = _processor_class_for_topic(topic)
        if not processor_cls:
            raise HTTPException(status_code=404, detail=f"Topic {topic} not found in processor registry")
        worker_list = _ensure_topic_list(topic)
        new_worker = KafkaWorker(processor_cls(), topic)
        worker_list.append(new_worker)
        task = asyncio.create_task(new_worker.start())
        worker_supervisor.register_worker(new_worker, task)
        return new_worker

    # ── Page ──────────────────────────────────────────────────────────────

    @app.get("/admin")
    async def admin_panel(request: Request):
        """Render admin panel dashboard."""
        return templates.TemplateResponse(
            request=request,
            name="admin.html",
            context={"request": request},
        )

    # ── Overview / health ─────────────────────────────────────────────────

    @app.get("/api/admin/overview")
    async def get_overview():
        """Cluster health summary across all registered topics."""
        topics_out = []
        totals = {
            "topics": 0,
            "configured_workers": 0,
            "running": 0,
            "stopped": 0,
            "processed": 0,
            "retries": 0,
            "errors": 0,
            "dlq": 0,
            "lag": 0,
        }

        for topic, meta in topic_registry.items():
            worker_entries = await _merge_topic_workers(topic)
            running = sum(1 for w in worker_entries if w["status"] == "running")
            stopped = sum(1 for w in worker_entries if w["status"] != "running")
            metrics = await _safe_metrics(topic)
            configured = meta["instances"]
            running_lags = [
                w["lag"] for w in worker_entries
                if w.get("status") == "running" and w.get("lag") is not None
            ]
            total_lag = sum(running_lags) if running_lags else None

            topics_out.append({
                "topic": topic,
                "processor": meta["processor_name"],
                "configured_workers": configured,
                "running": running,
                "stopped": stopped,
                "workers": worker_entries,
                "metrics": metrics,
                "lag_total": total_lag,
                "healthy": running > 0,
            })

            totals["topics"] += 1
            totals["configured_workers"] += configured
            totals["running"] += running
            totals["stopped"] += stopped
            totals["processed"] += metrics.get("processed", 0)
            totals["retries"] += metrics.get("retries", 0)
            totals["errors"] += metrics.get("errors", 0)
            totals["dlq"] += metrics.get("dlq", 0)
            if total_lag is not None:
                totals["lag"] = totals.get("lag", 0) + total_lag

        redis_ok = True
        try:
            if redis_client.redis is None:
                redis_ok = False
            else:
                await redis_client.redis.ping()
        except Exception:
            redis_ok = False

        return {
            "status": "healthy" if redis_ok and totals["running"] > 0 else (
                "degraded" if redis_ok else "unhealthy"
            ),
            "redis_ok": redis_ok,
            "totals": totals,
            "topics": topics_out,
        }

    @app.get("/api/admin/topics")
    async def list_topics():
        """List registered processors/topics with worker + metric snapshots."""
        overview = await get_overview()
        return overview["topics"]

    @app.get("/api/admin/supervisor")
    async def get_supervisor_status():
        """Get status of background worker supervisor and task health."""
        return worker_supervisor.get_health_status()

    @app.get("/api/admin/health")
    async def get_health_status():
        """Get system health check status (Redis + Worker Supervisor)."""
        redis_ok = True
        redis_error = None
        try:
            if redis_client.redis is None:
                redis_ok = False
                redis_error = "Redis client not initialized"
            else:
                await redis_client.redis.ping()
        except Exception as e:
            redis_ok = False
            redis_error = str(e)

        sup_health = worker_supervisor.get_health_status()
        kafka_errors = [
            {
                "topic": w["topic"],
                "worker_id": w["worker_id"],
                "state": w["state"],
                "error": w["last_error"],
                "consecutive_errors": w.get("consecutive_errors", 0),
            }
            for w in sup_health.get("workers", [])
            if w.get("last_error") or w.get("state") in ("degraded", "restarting", "failed")
        ]
        kafka_ok = len(kafka_errors) == 0

        is_unhealthy = not redis_ok or sup_health["status"] == "unhealthy"
        is_degraded = not is_unhealthy and (sup_health["status"] == "degraded" or not kafka_ok)
        overall_status = "unhealthy" if is_unhealthy else ("degraded" if is_degraded else "healthy")

        return {
            "status": overall_status,
            "status_code": 503 if is_unhealthy else 200,
            "redis": {
                "status": "connected" if redis_ok else "disconnected",
                "error": redis_error,
            },
            "kafka": {
                "status": "connected" if kafka_ok else ("error" if is_unhealthy else "degraded"),
                "active_workers": sup_health.get("active_workers", 0),
                "errors": kafka_errors,
            },
            "supervisor": sup_health,
        }

    @app.get("/api/admin/topics/{topic}")
    async def get_topic(topic: str):
        """Detail for a single topic."""
        if topic not in topic_registry:
            raise HTTPException(status_code=404, detail=f"Topic {topic} not registered")
        meta = topic_registry[topic]
        workers_list = await _merge_topic_workers(topic)
        metrics = await _safe_metrics(topic)
        return {
            "topic": topic,
            "processor": meta["processor_name"],
            "configured_workers": meta["instances"],
            "workers": workers_list,
            "metrics": metrics,
            "running": sum(1 for w in workers_list if w["status"] == "running"),
            "stopped": sum(1 for w in workers_list if w["status"] != "running"),
        }

    # ── Workers ───────────────────────────────────────────────────────────

    @app.get("/api/admin/topics/{topic}/workers")
    async def list_topic_workers(topic: str):
        """List all workers for a topic (in-memory + Redis orphans)."""
        if topic not in topic_registry and topic not in workers:
            raise HTTPException(status_code=404, detail=f"Topic {topic} not found")
        return await _merge_topic_workers(topic)

    @app.post("/api/admin/topics/{topic}/workers/{worker_id}/start")
    async def start_worker(topic: str, worker_id: str):
        """Restart a specific stopped worker (keeps list slot; new worker_id issued)."""
        try:
            idx, worker = _find_worker(topic, worker_id)
            if worker is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Worker {worker_id} for topic {topic} not found in memory. "
                           f"Use start-all or spawn to create a new worker.",
                )
            if worker.running:
                return {"status": "already_running", "worker_id": worker_id, "topic": topic}

            new_worker = await _restart_worker_at(topic, idx)
            logger.info(f"Restarted worker for topic={topic} old_id={worker_id} new_id={new_worker.worker_id}")
            return {
                "status": "started",
                "topic": topic,
                "old_worker_id": worker_id,
                "worker_id": new_worker.worker_id,
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error starting worker {topic}:{worker_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/admin/topics/{topic}/workers/{worker_id}/stop")
    async def stop_worker(topic: str, worker_id: str):
        """Stop a specific in-memory worker by worker_id."""
        try:
            idx, worker = _find_worker(topic, worker_id)
            if worker is None:
                raise HTTPException(
                    status_code=404,
                    detail=f"Worker {worker_id} for topic {topic} not found",
                )
            if not worker.running:
                return {"status": "already_stopped", "worker_id": worker_id, "topic": topic}

            await worker.stop()
            logger.info(f"Stopped worker topic={topic} worker_id={worker_id}")
            return {"status": "stopped", "worker_id": worker_id, "topic": topic}
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error stopping worker {topic}:{worker_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/admin/topics/{topic}/workers/start-all")
    async def start_all_workers(topic: str):
        """Start/restart all stopped in-memory workers for a topic.

        If the in-memory list is empty, spawn ``configured_workers`` new instances.
        """
        if topic not in topic_registry:
            raise HTTPException(status_code=404, detail=f"Topic {topic} not registered")

        try:
            worker_list = _ensure_topic_list(topic)
            started = []
            already_running = []

            if not worker_list:
                configured = topic_registry[topic]["instances"]
                for _ in range(configured):
                    w = await _spawn_worker(topic)
                    started.append(w.worker_id)
            else:
                for idx, worker in list(enumerate(worker_list)):
                    if worker.running:
                        already_running.append(worker.worker_id)
                        continue
                    new_worker = await _restart_worker_at(topic, idx)
                    started.append(new_worker.worker_id)

            logger.info(f"start-all topic={topic} started={started} already_running={already_running}")
            return {
                "status": "ok",
                "topic": topic,
                "started": started,
                "already_running": already_running,
            }
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error in start-all for {topic}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/admin/topics/{topic}/workers/stop-all")
    async def stop_all_workers(topic: str):
        """Stop all in-memory workers for a topic."""
        worker_list = workers.get(topic) or []
        if not worker_list:
            raise HTTPException(status_code=404, detail=f"No in-memory workers for topic {topic}")

        stopped = []
        already_stopped = []
        errors = []
        for worker in worker_list:
            try:
                if not worker.running:
                    already_stopped.append(worker.worker_id)
                    continue
                await worker.stop()
                stopped.append(worker.worker_id)
            except Exception as e:
                logger.error(f"Error stopping {topic}:{worker.worker_id}: {e}")
                errors.append({"worker_id": worker.worker_id, "error": str(e)})

        return {
            "status": "ok" if not errors else "partial",
            "topic": topic,
            "stopped": stopped,
            "already_stopped": already_stopped,
            "errors": errors,
        }

    @app.post("/api/admin/topics/{topic}/workers/spawn")
    async def spawn_worker(topic: str):
        """Spawn one additional worker for a topic (appends to the list)."""
        if topic not in topic_registry:
            raise HTTPException(status_code=404, detail=f"Topic {topic} not registered")
        try:
            worker = await _spawn_worker(topic)
            logger.info(f"Spawned extra worker topic={topic} worker_id={worker.worker_id}")
            return {"status": "started", "topic": topic, "worker_id": worker.worker_id}
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error spawning worker for {topic}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=str(e))

    @app.post("/api/admin/topics/{topic}/seek-to-time")
    async def seek_topic_to_time(topic: str, req: SeekToTimeRequest):
        """Seek all running workers for a topic to the specified datetime/timestamp."""
        if topic not in topic_registry and topic not in workers:
            raise HTTPException(status_code=404, detail=f"Topic {topic} not registered")

        dt = parse_target_datetime(req.datetime_str, req.timezone_str)
        timestamp_ms = int(dt.timestamp() * 1000)
        utc_iso = dt.astimezone(timezone.utc).isoformat()

        worker_list = workers.get(topic) or []
        active_workers = [w for w in worker_list if w.running and getattr(w, "consumer", None)]
        if not active_workers:
            raise HTTPException(
                status_code=400,
                detail=f"No active running workers with initialized consumer found for topic {topic}. Start workers first.",
            )

        tasks = [w.seek_to_time(timestamp_ms, reset_state=req.reset_state) for w in active_workers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        worker_results = []
        for worker, res in zip(active_workers, results):
            if isinstance(res, Exception):
                logger.error(f"Error seeking worker {topic}:{worker.worker_id}: {res}")
                worker_results.append({
                    "worker_id": worker.worker_id,
                    "status": "error",
                    "error": str(res),
                    "seeked_partitions": {},
                })
            else:
                worker_results.append(res)

        logger.info(f"Seeked {len(active_workers)} worker(s) for topic {topic} to {dt} ({timestamp_ms} ms)")

        return {
            "status": "ok",
            "topic": topic,
            "target_datetime": dt.isoformat(),
            "target_utc": utc_iso,
            "target_timestamp_ms": timestamp_ms,
            "workers_count": len(active_workers),
            "results": worker_results,
        }

    # ── Metrics ───────────────────────────────────────────────────────────

    @app.get("/api/admin/topics/{topic}/metrics")
    async def get_topic_metrics(topic: str):
        """Processed / retry / error / DLQ counters for a topic."""
        return await _safe_metrics(topic)

    @app.post("/api/admin/topics/{topic}/metrics/reset")
    async def reset_topic_metrics(topic: str):
        """Reset Redis metric counters for a topic."""
        try:
            await redis_client.reset_metrics(topic)
            return {"status": "reset", "topic": topic}
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))

    # ── Logs ──────────────────────────────────────────────────────────────

    @app.get("/api/admin/topics/{topic}/workers/{worker_id}/logs")
    async def get_worker_logs(
        topic: str,
        worker_id: str,
        limit: int = Query(default=200, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ):
        """Get recent logs for a worker (newest first)."""
        try:
            logs = await redis_client.get_logs(topic, worker_id, limit=limit, offset=offset)
            total = await redis_client.get_log_count(topic, worker_id)
            return {
                "topic": topic,
                "worker_id": worker_id,
                "total": total,
                "limit": limit,
                "offset": offset,
                "logs": logs,
            }
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))

    @app.delete("/api/admin/topics/{topic}/workers/{worker_id}/logs")
    async def clear_worker_logs(topic: str, worker_id: str):
        """Clear stored logs for a worker."""
        try:
            await redis_client.clear_logs(topic, worker_id)
            return {"status": "cleared", "topic": topic, "worker_id": worker_id}
        except RuntimeError as e:
            raise HTTPException(status_code=503, detail=str(e))

    # ── Backward-compatible aliases (old UI / scripts) ────────────────────

    @app.get("/api/workers")
    async def get_workers_legacy():
        """Legacy shape: {topic: {worker_id: {status, partitions}}}."""
        result = {}
        for topic in topic_registry:
            entries = await _merge_topic_workers(topic)
            result[topic] = {
                w["worker_id"]: {
                    "status": w["status"],
                    "partitions": w["partitions"],
                    "updated_at": w.get("updated_at"),
                }
                for w in entries
            }
        return result

    @app.get("/api/workers/{topic}/ids")
    async def get_worker_ids_legacy(topic: str):
        entries = await _merge_topic_workers(topic)
        return [w["worker_id"] for w in entries]

    @app.get("/api/logs/{topic}/{worker_id}")
    async def get_logs_legacy(topic: str, worker_id: str):
        try:
            return await redis_client.get_logs(topic, worker_id, limit=1000, offset=0)
        except RuntimeError as e:
            return {"error": "Redis client not initialized", "details": str(e)}

    @app.post("/api/workers/{topic}/{worker_id}/start")
    async def start_worker_legacy(topic: str, worker_id: str):
        return await start_worker(topic, worker_id)

    @app.post("/api/workers/{topic}/{worker_id}/stop")
    async def stop_worker_legacy(topic: str, worker_id: str):
        return await stop_worker(topic, worker_id)


def _build_topic_registry() -> dict:
    from kafka_pilot.processors import MessageProcessor

    registry = {}
    for cls in MessageProcessor.registry.values():
        instance = cls()
        topics = [t.strip() for t in instance.topic_conf.topics.split(",") if t.strip()]
        for topic in topics:
            registry[topic] = {
                "processor_class": cls,
                "processor_name": cls.__name__,
                "instances": getattr(cls, "instances", 1),
            }
    return registry


async def _safe_metrics(topic: str) -> dict:
    try:
        return await redis_client.get_metrics(topic)
    except RuntimeError:
        return {"processed": 0, "retries": 0, "errors": 0, "dlq": 0, "error": "redis_unavailable"}
