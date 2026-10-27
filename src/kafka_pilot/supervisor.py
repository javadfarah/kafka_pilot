"""Worker supervisor for tracking, detecting, and auto-healing Kafka worker tasks."""
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
import time
from typing import Any, Dict, List, Optional
from kafka_pilot.utils import logger

DEFAULT_CHECK_INTERVAL_SECONDS = 30.0
DEFAULT_STARTUP_GRACE_PERIOD_SECONDS = 60.0
MAX_AUTO_RESTARTS = 5


@dataclass
class SupervisedWorker:
    topic: str
    worker_id: str
    worker: Any
    task: Optional[asyncio.Task]
    restart_count: int = 0
    max_retries: int = MAX_AUTO_RESTARTS
    last_restart_at: Optional[datetime] = None
    last_error: Optional[str] = None
    consecutive_errors: int = 0
    state: str = "running"  # running | restarting | degraded | stopped | failed


class WorkerSupervisor:
    """Monitors worker tasks locally within the process, detects crashes, and auto-restarts failed consumers."""

    _instance: Optional["WorkerSupervisor"] = None

    @classmethod
    def get_instance(cls) -> "WorkerSupervisor":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(
        self,
        check_interval_seconds: float = DEFAULT_CHECK_INTERVAL_SECONDS,
        startup_grace_period_seconds: float = DEFAULT_STARTUP_GRACE_PERIOD_SECONDS,
        apply_backoff: bool = True,
    ):
        self.check_interval_seconds = check_interval_seconds
        self.startup_grace_period_seconds = startup_grace_period_seconds
        self.apply_backoff = apply_backoff
        self._workers: Dict[str, SupervisedWorker] = {}
        self._supervise_task: Optional[asyncio.Task] = None
        self._running: bool = False
        self._started_at: float = 0.0

    def is_running(self) -> bool:
        return self._running

    def register_worker(self, worker: Any, task: Optional[asyncio.Task] = None) -> None:
        """Register a worker instance and its execution task for supervision."""
        worker_id = getattr(worker, "worker_id", str(id(worker)))
        topic = getattr(worker, "topic", "unknown")
        state = "running" if getattr(worker, "running", True) else "stopped"

        self._workers[worker_id] = SupervisedWorker(
            topic=topic,
            worker_id=worker_id,
            worker=worker,
            task=task,
            state=state,
        )
        logger.info(f"[WorkerSupervisor] Registered worker {topic}:{worker_id} (task={task is not None})")

    def unregister_worker(self, worker_id: str) -> None:
        """Unregister a worker from supervision."""
        if worker_id in self._workers:
            del self._workers[worker_id]
            logger.info(f"[WorkerSupervisor] Unregistered worker {worker_id}")

    def get_supervised_worker(self, worker_id: str) -> Optional[SupervisedWorker]:
        return self._workers.get(worker_id)

    async def check_workers(self) -> None:
        """Inspect all registered workers and restart any dead tasks up to 5 retries."""
        # Respect initial startup grace period before triggering restarts
        if self._started_at and (time.monotonic() - self._started_at < self.startup_grace_period_seconds):
            return

        for worker_id, supervised in list(self._workers.items()):
            worker = supervised.worker
            task = supervised.task
            should_be_running = getattr(worker, "running", True)

            # If task was never assigned or is done
            if task is None or task.done():
                if not should_be_running:
                    supervised.state = "stopped"
                    continue

                # Worker is supposed to be running, but task has terminated!
                exc = None
                if task is not None and not task.cancelled():
                    try:
                        exc = task.exception()
                    except (asyncio.InvalidStateError, asyncio.CancelledError):
                        exc = None

                error_msg = str(exc) if exc is not None else "Task completed unexpectedly without exception"
                supervised.last_error = error_msg
                supervised.last_restart_at = datetime.now(timezone.utc)

                if supervised.restart_count >= supervised.max_retries:
                    supervised.state = "failed"
                    logger.critical(
                        f"[WorkerSupervisor] Worker {supervised.topic}:{worker_id} exceeded max retries "
                        f"({supervised.max_retries}). Marked as permanently failed. Error: {error_msg}"
                    )
                    continue

                supervised.restart_count += 1
                supervised.state = "restarting"
                backoff_seconds = min(2 ** (supervised.restart_count - 1), 60) if self.apply_backoff else 0

                logger.error(
                    f"[WorkerSupervisor] Detected dead worker {supervised.topic}:{worker_id}. "
                    f"Error: {error_msg}. Restart attempt #{supervised.restart_count}/{supervised.max_retries} "
                    f"with {backoff_seconds}s backoff..."
                )

                # Schedule restart with backoff
                async def _restart_with_backoff(w=worker, s=supervised, delay=backoff_seconds):
                    if delay > 0:
                        await asyncio.sleep(delay)
                    if not getattr(w, "running", True):
                        return
                    new_task = asyncio.create_task(w.start())
                    s.task = new_task
                    s.state = "running"
                    logger.info(f"[WorkerSupervisor] Successfully respawned task for {s.topic}:{s.worker_id}")

                asyncio.create_task(_restart_with_backoff())
            else:
                # Task is running; inspect worker internal health and error telemetry
                worker_last_error = getattr(worker, "last_error", None)
                if not isinstance(worker_last_error, str):
                    worker_last_error = None
                worker_last_error_at = getattr(worker, "last_error_at", None)
                if not isinstance(worker_last_error_at, datetime):
                    worker_last_error_at = None
                consecutive_errors = getattr(worker, "consecutive_errors", 0)
                if not isinstance(consecutive_errors, int) or isinstance(consecutive_errors, bool):
                    consecutive_errors = 0

                supervised.last_error = worker_last_error
                supervised.consecutive_errors = consecutive_errors
                if worker_last_error_at:
                    supervised.last_restart_at = worker_last_error_at

                if consecutive_errors >= 3:
                    supervised.state = "degraded"
                else:
                    supervised.state = "running"

    async def _supervise_loop(self) -> None:
        """Periodic supervision loop running every 10s."""
        try:
            while self._running:
                await asyncio.sleep(self.check_interval_seconds)
                if not self._running:
                    break
                try:
                    await self.check_workers()
                except Exception as err:
                    logger.error(f"[WorkerSupervisor] Error during supervision cycle: {err}", exc_info=True)
        except asyncio.CancelledError:
            pass

    async def start(self) -> None:
        """Start the background supervision loop."""
        if self._running:
            return
        self._running = True
        self._started_at = time.monotonic()
        self._supervise_task = asyncio.create_task(self._supervise_loop())
        logger.info(
            f"[WorkerSupervisor] Supervisor started successfully (check_interval={self.check_interval_seconds}s, "
            f"grace_period={self.startup_grace_period_seconds}s)"
        )

    async def stop(self) -> None:
        """Stop the background supervision loop."""
        self._running = False
        if self._supervise_task is not None:
            self._supervise_task.cancel()
            try:
                await self._supervise_task
            except asyncio.CancelledError:
                pass
            self._supervise_task = None
        logger.info("[WorkerSupervisor] Supervisor stopped")

    def get_health_status(self) -> dict:
        """Return comprehensive health status of all supervised workers."""
        total = len(self._workers)
        active = 0
        restarting = 0
        degraded = 0
        failed = 0
        stopped = 0
        worker_summaries: List[dict] = []

        for worker_id, s in self._workers.items():
            # Synchronize latest worker error state if available
            worker = getattr(s, "worker", None)
            if worker is not None:
                worker_last_error = getattr(worker, "last_error", None)
                if isinstance(worker_last_error, str):
                    s.last_error = worker_last_error
                    worker_last_error_at = getattr(worker, "last_error_at", None)
                    if isinstance(worker_last_error_at, datetime):
                        s.last_restart_at = worker_last_error_at
                raw_errors = getattr(worker, "consecutive_errors", 0)
                if isinstance(raw_errors, int) and not isinstance(raw_errors, bool):
                    s.consecutive_errors = raw_errors
                else:
                    s.consecutive_errors = 0

                if s.consecutive_errors >= 3 and s.state == "running":
                    s.state = "degraded"

            if s.state == "running":
                active += 1
            elif s.state == "degraded":
                degraded += 1
            elif s.state == "restarting":
                restarting += 1
            elif s.state == "failed":
                failed += 1
            elif s.state == "stopped":
                stopped += 1

            worker_summaries.append({
                "topic": s.topic,
                "worker_id": s.worker_id,
                "state": s.state,
                "restart_count": s.restart_count,
                "max_retries": s.max_retries,
                "consecutive_errors": s.consecutive_errors,
                "last_restart_at": s.last_restart_at.isoformat() if s.last_restart_at else None,
                "last_error": s.last_error,
            })

        overall_status = "healthy"
        if failed > 0:
            overall_status = "unhealthy"
        elif restarting > 0 or degraded > 0:
            overall_status = "degraded"
        elif total > 0 and active == 0 and stopped < total:
            overall_status = "unhealthy"

        return {
            "status": overall_status,
            "supervisor_running": self._running,
            "total_workers": total,
            "active_workers": active,
            "degraded_workers": degraded,
            "restarting_workers": restarting,
            "failed_workers": failed,
            "stopped_workers": stopped,
            "workers": worker_summaries,
        }


worker_supervisor = WorkerSupervisor.get_instance()
