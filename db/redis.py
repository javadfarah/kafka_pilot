import datetime
import json
from typing import Set, List
import orjson
import redis.asyncio as async_redis_client
from kafka_pilot.utils import logger


class RedisClient:
    """Manages Redis operations ."""

    _instance = None

    @classmethod
    def get_instance(cls):
        """Get or create the singleton instance of RedisClient."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):

        """Initialize constructor (called only once due to singleton)."""
        if RedisClient._instance is not None:
            raise Exception("This class is a singleton. Use get_instance() instead.")
        self.redis = None
        from kafka_pilot.config import get_conf

        self.config = get_conf()

    async def initialize(self):
        """Initialize Redis connection if not already initialized."""
        if self.redis is None:
            try:
                self.redis = async_redis_client.Redis(host=self.config.KFP__REDIS_HOST,
                                                      port=self.config.KFP__REDIS_PORT,
                                                      db=self.config.KFP__REDIS_DB_NUMBER,
                                                      password=self.config.KFP__REDIS_PASSWORD, decode_responses=True)
                # Test connection
                await self.redis.ping()
                logger.info("Redis connection initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Redis connection: {e}")
                self.redis = None
                raise RuntimeError(f"Redis initialization failed: {e}")

    async def close(self):
        """Close Redis connection."""
        if self.redis:
            try:
                await self.redis.close()
                logger.info("Redis connection closed")
            except Exception as e:
                logger.error(f"Error closing Redis connection: {e}")
            finally:
                self.redis = None

    async def set_worker_status(self, topic: str, worker_id: str, status: str, partitions: Set[int]):
        """Set worker status and partitions in Redis."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        key = f"worker:{topic}:{worker_id}"
        await self.redis.hset(key, mapping={
            "status": status,
            "partitions": json.dumps(list(partitions))
        })
        # Set TTL only for stopped/failed workers
        if status in ["stopped", "failed"]:
            await self.redis.expire(key, 3600)
        else:
            # Remove TTL for running workers to prevent expiration
            await self.redis.persist(key)

    async def get_worker_status(self, topic: str) -> dict:
        """Get status of active workers for a topic."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        workers = {}
        keys = await self.redis.keys(f"worker:{topic}:*")
        for key in keys:
            worker_id = key.split(":")[-1]
            data = await self.redis.hgetall(key)
            # Only include status for running workers
            if data.get("status") == "running":
                workers[worker_id] = {
                    "status": data.get("status", "unknown"),
                    "partitions": json.loads(data.get("partitions", "[]"))
                }
        return workers

    async def get_all_worker_ids(self, topic: str) -> List[str]:
        """Get all worker IDs for a topic, including stopped/failed ones, for log access."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        keys = await self.redis.keys(f"worker:{topic}:*")
        return [key.split(":")[-1] for key in keys]

    async def modify_topic_old_workers(self, topic: str):
        workers_list = await self.get_all_worker_ids(topic)
        _ = set()
        for worker_id in workers_list:
            await self.set_worker_status(topic=topic, worker_id=worker_id, status="stopped", partitions=_)

    async def log_message(self, topic: str, worker_id: str, message: str):
        """Log a message for a worker persistently."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        key = f"logs:{topic}:{worker_id}"
        await self.redis.lpush(key, message)
        # Keep last 1000 logs to manage space
        await self.redis.ltrim(key, 0, 999)

    async def get_logs(self, topic: str, worker_id: str) -> list:
        """Get logs for a worker."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        key = f"logs:{topic}:{worker_id}"
        return await self.redis.lrange(key, 0, -1)

    async def increment_processed(self, topic: str):
        """Increment processed messages counter."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        await self.redis.incr(f"metrics:{topic}:processed")

    async def increment_retry(self, topic: str):
        """Increment retry counter."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        await self.redis.incr(f"metrics:{topic}:retries")

    async def increment_error(self, topic: str):
        """Increment error counter."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        await self.redis.incr(f"metrics:{topic}:errors")

    async def increment_dlq(self, topic: str):
        """Increment DLQ counter."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        await self.redis.incr(f"metrics:{topic}:dlq")

    async def get_metrics(self, topic: str) -> dict:
        """Get metrics for a topic."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        return {
            "processed": int(await self.redis.get(f"metrics:{topic}:processed") or 0),
            "retries": int(await self.redis.get(f"metrics:{topic}:retries") or 0),
            "errors": int(await self.redis.get(f"metrics:{topic}:errors") or 0),
            "dlq": int(await self.redis.get(f"metrics:{topic}:dlq") or 0)
        }

    async def set_data(self, key: str, value: dict) -> None:
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        data = json.dumps(value)
        await self.redis.set(key, data, 60 * 60 * 24)

    async def bulk_insert(self, data: dict) -> None:

        pipe = self.redis.pipeline()
        for key, value in data.items():
            pipe.set(key, value)

        await pipe.execute()

    async def bulk_insert_hashes(self, data: dict):
        pipe = self.redis.pipeline()
        for redis_key, value in data.items():
            mapping = {str(k): orjson.dumps(v).decode() for k, v in value.items()}
            pipe.hset(redis_key, mapping=mapping)

        await pipe.execute()

    async def insert_hash(self, key: str, data: dict):
        mapping = {str(k): orjson.dumps(v).decode() for k, v in data.items()}
        await self.redis.hset(key, mapping=mapping)

    async def execute_script(self, script: str, numkeys: int, keys_and_args: str):
        return await self.redis.eval(script, numkeys, *keys_and_args.split(','))

    async def zadd_value(self, key: str, value: str, timestamp: datetime.time):
        await self.redis.zadd(key, {value: timestamp})


redis_client = RedisClient.get_instance()
