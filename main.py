import asyncio
from contextlib import asynccontextmanager
from typing import Dict

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from kafka_pilot.db import redis_client
from kafka_pilot.supervisor import worker_supervisor
from kafka_pilot.utils import logger
from kafka_pilot.web_admin import setup_web_admin
from kafka_pilot.worker import KafkaWorker

# Global workers dictionary: topic -> list of workers (supports multi-worker per topic)
workers: dict[str, list[KafkaWorker]] = {}

console = Console(force_terminal=True, color_system="auto")


def print_initiation_text(tasks: Dict):
    console.print(Panel.fit(f"""
 █▄▀ ▄▀█ █▀▀ █▄▀ ▄▀█   █▀█ █ █   █▀█ ▀█▀
 █ █ █▀█ █▀  █ █ █▀█   █▀  █ █▄▄ █▄█  █ 
                             
""",
                            title="[yellow]🚀kafka pilot started[/yellow]",
                            border_style="bright_blue"
                            ))
    table = Table(title="[bold magenta]Registered Tasks[/bold magenta]", show_lines=True, header_style="bold magenta",
                  border_style="bright_blue")
    table.add_column("Topic", style="cyan", justify="left", overflow="fold")
    table.add_column("Processor class", style="green", overflow="fold")

    for topic, task_class in tasks.items():
        table.add_row(str(topic), str(task_class.__class__))

    console.print(table)
    console.print("Color support:", console.color_system)


async def kafka_pilot_startup():
    from kafka_pilot.processors import MessageProcessor
    processors_per_topic = {
        topic.strip(): {
            "processor": cls(),  # new instance per topic
            "worker_count": cls.instances  # class-level attribute
        }
        for cls in MessageProcessor.registry.values()
        for topic in cls().topic_conf.topics.split(',')
    }
    print_initiation_text(processors_per_topic)
    try:
        # Initialize Redis
        await redis_client.initialize()

        # Records from a previous process keep their last status, so clear them
        # before this run registers its own workers.
        for topic in processors_per_topic:
            stale = await redis_client.mark_all_workers_stopped(topic)
            if stale:
                logger.info(f"Marked {len(stale)} stale worker record(s) stopped for topic: {topic}")
        # Start workers for each topic
        for topic, data in processors_per_topic.items():
            processor = data["processor"]
            worker_count = data["worker_count"]

            workers[topic] = []  # store multiple workers per topic

            for i in range(worker_count):
                worker = KafkaWorker(processor=processor, topic=topic)  # optional id
                workers[topic].append(worker)
                task = asyncio.create_task(worker.start())
                worker_supervisor.register_worker(worker, task)
                logger.info(f"Started worker {i + 1}/{worker_count} for topic: {topic}")

        # Start background worker supervisor
        await worker_supervisor.start()

    except Exception as e:
        logger.error(f"Error during startup: {e}", exc_info=True)

        # Stop supervisor first
        await worker_supervisor.stop()

        # Graceful shutdown
        shutdown_tasks = [
            worker.stop()
            for worker_list in workers.values()
            for worker in worker_list  # flatten the nested list
        ]
        await asyncio.gather(*shutdown_tasks, return_exceptions=True)

        await redis_client.close()
        logger.info("All workers and Redis stopped gracefully")


async def kafka_pilot_shutdown():
    # Stop background supervisor first so it does not auto-restart stopping workers
    await worker_supervisor.stop()

    # Graceful shutdown
    shutdown_tasks = [
        worker.stop()
        for worker_list in workers.values()
        for worker in worker_list
    ]

    await asyncio.gather(*shutdown_tasks, return_exceptions=True)
    await redis_client.close()
    logger.info("All workers and Redis stopped gracefully")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown lifecycle."""

    await kafka_pilot_startup()
    yield
    await kafka_pilot_shutdown()


app = FastAPI(lifespan=lifespan)
setup_web_admin(app, workers)  # Pass workers to web_admin


@app.get("/health")
async def health_check():
    """Health check endpoint for the application and Kubernetes probes."""
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

    status_code = 503 if is_unhealthy else 200
    overall_status = "unhealthy" if is_unhealthy else ("degraded" if is_degraded else "healthy")

    payload = {
        "status": overall_status,
        "status_code": status_code,
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
    return JSONResponse(status_code=status_code, content=payload)


def main():
    """Run the FastAPI application with uvicorn."""
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
