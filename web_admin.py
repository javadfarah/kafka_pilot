import asyncio
import json

from fastapi import FastAPI
from fastapi.requests import Request
from fastapi.templating import Jinja2Templates

from kafka_pilot.config import get_conf
from kafka_pilot.db import redis_client
from kafka_pilot.utils import logger
from kafka_pilot.worker import KafkaWorker


def setup_web_admin(app: FastAPI, workers: dict):
    """Set up web admin panel routes."""
    from kafka_pilot.processors import MessageProcessor
    topics = {
        topic.strip(): cls
        for cls in MessageProcessor.registry.values()
        for topic in cls().topic_conf.topics.split(',')
    }

    templates = Jinja2Templates(directory="kafka_pilot/templates")

    @app.get("/admin")
    async def admin_panel(request: Request):
        """Render admin panel dashboard."""
        return templates.TemplateResponse("admin.html", {"request": request})

    @app.get("/api/workers")
    async def get_workers():
        """Get status of all workers (running and stopped)."""
        workers_status = {}
        try:
            for topic in topics.keys():
                workers_status[topic] = await redis_client.get_worker_status(topic)
                # Include stopped/failed workers
                all_worker_ids = await redis_client.get_all_worker_ids(topic)
                for worker_id in all_worker_ids:
                    if worker_id not in workers_status[topic]:
                        try:
                            data = await redis_client.redis.hgetall(f"worker:{topic}:{worker_id}")
                            workers_status[topic][worker_id] = {
                                "status": data.get("status", "unknown"),
                                "partitions": json.loads(data.get("partitions", "[]"))
                            }
                        except Exception as e:
                            logger.error(f"Error fetching worker status for {topic}:{worker_id}: {e}")
                            workers_status[topic][worker_id] = {
                                "status": "error",
                                "partitions": [],
                                "error": str(e)
                            }
            return workers_status
        except RuntimeError as e:
            logger.error(f"Redis client error in get_workers: {e}")
            return {"error": "Redis client not initialized", "details": str(e)}

    @app.get("/api/workers/{topic}/ids")
    async def get_worker_ids(topic: str):
        """Get all worker IDs for a topic, including stopped/failed ones."""
        try:
            return await redis_client.get_all_worker_ids(topic)
        except RuntimeError as e:
            logger.error(f"Redis client error in get_worker_ids: {e}")
            return {"error": "Redis client not initialized", "details": str(e)}

    @app.get("/api/logs/{topic}/{worker_id}")
    async def get_logs(topic: str, worker_id: str):
        """Get logs for a specific worker."""
        try:
            return await redis_client.get_logs(topic, worker_id)
        except RuntimeError as e:
            logger.error(f"Redis client error in get_logs: {e}")
            return {"error": "Redis client not initialized", "details": str(e)}

    @app.post("/api/workers/{topic}/{worker_id}/start")
    async def start_worker(topic: str, worker_id: str):
        """Start or restart a worker."""
        try:
            if topic in workers and workers[topic].worker_id == worker_id:
                if workers[topic].running:
                    return {"status": "already_running"}
                else:
                    # Restart the worker by creating a new instance
                    topic_class = topics.get(topic)
                    if topic_class:
                        workers[topic] = KafkaWorker(topic_class(), topic)
                        asyncio.create_task(workers[topic].start())
                        logger.info(f"Restarted worker for topic: {topic}, worker_id: {worker_id}")
                        return {"status": "started"}
                    else:
                        return {"status": "error", "message": f"Topic {topic} not found in configuration"}
            else:
                # Create a new worker if it doesn't exist or worker_id doesn't match
                topic_class = topics.get(topic)
                if topic_class:
                    workers[topic] = KafkaWorker(topic_class(), topic)
                    asyncio.create_task(workers[topic].start())
                    logger.info(f"Started new worker for topic: {topic}, worker_id: {worker_id}")
                    return {"status": "started"}
                else:
                    return {"status": "error", "message": f"Topic {topic} not found in configuration"}
        except Exception as e:
            logger.error(f"Error starting worker {topic}:{worker_id}: {e}")
            return {"status": "error", "message": str(e)}

    @app.post("/api/workers/{topic}/{worker_id}/stop")
    async def stop_worker(topic: str, worker_id: str):
        """Stop a worker."""
        try:
            if topic in workers and workers[topic].worker_id == worker_id:
                await workers[topic].stop()
                return {"status": "stopped"}
            return {"status": "error", "message": f"Worker {worker_id} for topic {topic} not found"}
        except Exception as e:
            logger.error(f"Error stopping worker {topic}:{worker_id}: {e}")
            return {"status": "error", "message": str(e)}
