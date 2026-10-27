import datetime
import json
from typing import Set, List
import orjson
import redis.asyncio as async_redis_client
from redis.asyncio.sentinel import Sentinel as AsyncSentinel
from kafka_pilot.utils import logger

# A running worker holds a lease that it must renew; if the process dies the key
# expires instead of advertising a worker that no longer exists.
WORKER_LEASE_SECONDS = 90
WORKER_HEARTBEAT_SECONDS = 30
TERMINAL_STATUS_TTL_SECONDS = 3600
TERMINAL_STATUSES = ("stopped", "failed")


def _with_auth(kwargs: dict, *, username, password) -> dict:
    """AUTH needs a password; a username on its own is dropped rather than sent."""
    if not password:
        return kwargs
    if username:
        kwargs["username"] = username
    kwargs["password"] = password
    return kwargs


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
        self.sentinel = None
        from kafka_pilot.config import get_conf

        self.config = get_conf()

    def _prefixed(self, key: str) -> str:
        """Logical key -> physical key. Keys outside the prefix are ACL-denied."""
        prefix = getattr(self.config, "KFP__REDIS_KEY_PREFIX", None)
        if not isinstance(prefix, str) or not prefix or key.startswith(prefix):
            return key
        return prefix + key

    async def _scan_keys(self, pattern: str) -> List[str]:
        """List physical keys via SCAN with fallback to KEYS for compatibility."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        match = self._prefixed(pattern)
        keys: List[str] = []
        try:
            cursor = 0
            while True:
                res = await self.redis.scan(cursor, match=match, count=100)
                if not isinstance(res, (tuple, list)) or len(res) != 2:
                    break
                cursor, batch = res
                keys.extend(batch)
                if str(cursor) == "0" or cursor == 0:
                    break
        except Exception:
            pass

        if not keys and hasattr(self.redis, "keys"):
            try:
                res_keys = await self.redis.keys(match)
                if isinstance(res_keys, list):
                    return res_keys
            except Exception:
                pass

        return keys

    def _create_client(self):
        redis_kwargs = dict(
            db=self.config.KFP__REDIS_DB_NUMBER,
            decode_responses=True,
            socket_timeout=5.0,
            socket_connect_timeout=5.0,
            socket_keepalive=True,
            health_check_interval=15,
            retry_on_timeout=True,
        )
        _with_auth(
            redis_kwargs,
            username=self.config.KFP__REDIS_USERNAME,
            password=self.config.KFP__REDIS_PASSWORD,
        )
        if self.config.KFP__REDIS_USE_SENTINEL:
            # Sentinel authenticates separately; data-node credentials are not reused.
            sentinel_kwargs = _with_auth(
                {},
                username=self.config.KFP__REDIS_SENTINEL_USERNAME,
                password=self.config.KFP__REDIS_SENTINEL_PASSWORD,
            )
            # Kept on the instance: the discovery clients are separate from the
            # master client and have to be closed alongside it.
            self.sentinel = AsyncSentinel(
                self.config.sentinel_addresses(),
                socket_timeout=5,
                sentinel_kwargs=sentinel_kwargs or None,
            )
            return self.sentinel.master_for(
                self.config.KFP__REDIS_SENTINEL_MASTER_NAME,
                **redis_kwargs,
            )
        self.sentinel = None
        return async_redis_client.Redis(
            host=self.config.KFP__REDIS_HOST,
            port=self.config.KFP__REDIS_PORT,
            **redis_kwargs,
        )

    async def initialize(self):
        """Initialize Redis connection if not already initialized."""
        if self.redis is None:
            try:
                self.redis = self._create_client()
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
        sentinel = getattr(self, "sentinel", None)
        if sentinel is not None:
            self.sentinel = None
            for discovery_client in sentinel.sentinels:
                try:
                    await discovery_client.aclose()
                except Exception as e:
                    logger.error(f"Error closing Sentinel connection: {e}")

    async def set_worker_status(self, topic: str, worker_id: str, status: str, partitions: Set[int]):
        """Set worker status and partitions in Redis."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        key = self._prefixed(f"worker:{topic}:{worker_id}")
        await self.redis.hset(key, mapping={
            "status": status,
            "partitions": json.dumps(list(partitions)),
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
        })
        if status in TERMINAL_STATUSES:
            await self.redis.expire(key, TERMINAL_STATUS_TTL_SECONDS)
        else:
            await self.redis.expire(key, WORKER_LEASE_SECONDS)

    async def get_worker_status(self, topic: str) -> dict:
        """Get status of active (running) workers for a topic."""
        all_workers = await self.get_all_worker_details(topic)
        return {
            worker_id: data
            for worker_id, data in all_workers.items()
            if data.get("status") == "running"
        }

    async def get_all_worker_details(self, topic: str) -> dict:
        """Get all workers for a topic (running, stopped, failed)."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        workers = {}
        keys = await self._scan_keys(f"worker:{topic}:*")
        for key in keys:
            worker_id = key.split(":")[-1]
            data = await self.redis.hgetall(key)
            try:
                partitions = json.loads(data.get("partitions", "[]"))
            except (TypeError, json.JSONDecodeError):
                partitions = []
            workers[worker_id] = {
                "status": data.get("status", "unknown"),
                "partitions": partitions,
                "updated_at": data.get("updated_at"),
            }
        return workers

    async def get_all_worker_ids(self, topic: str) -> List[str]:
        """Get all worker IDs for a topic, including stopped/failed ones, for log access."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        keys = await self._scan_keys(f"worker:{topic}:*")
        return [key.split(":")[-1] for key in keys]

    async def mark_all_workers_stopped(self, topic: str) -> List[str]:
        """Mark every worker record for a topic as stopped.

        Workers get a fresh id on each run, so records left behind by a previous
        process would otherwise never be overwritten.
        """
        worker_ids = await self.get_all_worker_ids(topic)
        for worker_id in worker_ids:
            await self.set_worker_status(
                topic=topic, worker_id=worker_id, status="stopped", partitions=set()
            )
        return worker_ids

    async def log_message(self, topic: str, worker_id: str, message: str):
        """Log a message for a worker persistently."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        key = self._prefixed(f"logs:{topic}:{worker_id}")
        await self.redis.lpush(key, message)
        # Keep last 1000 logs to manage space
        await self.redis.ltrim(key, 0, 999)

    async def get_logs(self, topic: str, worker_id: str, limit: int = 200, offset: int = 0) -> list:
        """Get logs for a worker (newest first)."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        key = self._prefixed(f"logs:{topic}:{worker_id}")
        end = offset + max(limit, 1) - 1
        return await self.redis.lrange(key, offset, end)

    async def get_log_count(self, topic: str, worker_id: str) -> int:
        """Get total stored log entries for a worker."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        return int(await self.redis.llen(self._prefixed(f"logs:{topic}:{worker_id}")) or 0)

    async def clear_logs(self, topic: str, worker_id: str) -> None:
        """Clear stored logs for a worker."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        await self.redis.delete(self._prefixed(f"logs:{topic}:{worker_id}"))

    async def reset_metrics(self, topic: str) -> None:
        """Reset metrics counters for a topic."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        await self.redis.delete(
            self._prefixed(f"metrics:{topic}:processed"),
            self._prefixed(f"metrics:{topic}:retries"),
            self._prefixed(f"metrics:{topic}:errors"),
            self._prefixed(f"metrics:{topic}:dlq"),
        )

    async def set_topic_lag(self, topic: str, worker_id: str, lag: int) -> None:
        """Persist last observed consumer lag for a worker."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        key = self._prefixed(f"lag:{topic}:{worker_id}")
        await self.redis.hset(key, mapping={
            "lag": str(int(lag)),
            "updated_at": datetime.datetime.utcnow().isoformat() + "Z",
        })
        await self.redis.expire(key, WORKER_LEASE_SECONDS)

    async def delete_topic_lag(self, topic: str, worker_id: str) -> None:
        """Drop a worker's lag key so stopped consumers are not summed in the UI."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        await self.redis.delete(self._prefixed(f"lag:{topic}:{worker_id}"))

    async def get_topic_lags(self, topic: str) -> dict:
        """Get last observed lag per worker for a topic."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        result = {}
        keys = await self._scan_keys(f"lag:{topic}:*")
        for key in keys:
            worker_id = key.split(":")[-1]
            data = await self.redis.hgetall(key)
            try:
                lag = int(data.get("lag", 0))
            except (TypeError, ValueError):
                lag = 0
            result[worker_id] = {
                "lag": lag,
                "updated_at": data.get("updated_at"),
            }
        return result

    async def increment_processed(self, topic: str):
        """Increment processed messages counter."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        await self.redis.incr(self._prefixed(f"metrics:{topic}:processed"))

    async def increment_retry(self, topic: str):
        """Increment retry counter."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        await self.redis.incr(self._prefixed(f"metrics:{topic}:retries"))

    async def increment_error(self, topic: str):
        """Increment error counter."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        await self.redis.incr(self._prefixed(f"metrics:{topic}:errors"))

    async def increment_dlq(self, topic: str):
        """Increment DLQ counter."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        await self.redis.incr(self._prefixed(f"metrics:{topic}:dlq"))

    async def get_metrics(self, topic: str) -> dict:
        """Get metrics for a topic."""
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        return {
            "processed": int(await self.redis.get(self._prefixed(f"metrics:{topic}:processed")) or 0),
            "retries": int(await self.redis.get(self._prefixed(f"metrics:{topic}:retries")) or 0),
            "errors": int(await self.redis.get(self._prefixed(f"metrics:{topic}:errors")) or 0),
            "dlq": int(await self.redis.get(self._prefixed(f"metrics:{topic}:dlq")) or 0)
        }

    async def set_data(self, key: str, value: dict) -> None:
        if self.redis is None:
            raise RuntimeError("Redis client not initialized")
        data = json.dumps(value)
        await self.redis.set(self._prefixed(key), data, 60 * 60 * 24)

    async def bulk_insert(self, data: dict) -> None:

        pipe = self.redis.pipeline()
        for key, value in data.items():
            pipe.set(self._prefixed(key), value)

        await pipe.execute()

    async def bulk_insert_hashes(self, data: dict):
        pipe = self.redis.pipeline()
        for redis_key, value in data.items():
            mapping = {str(k): orjson.dumps(v).decode() for k, v in value.items()}
            pipe.hset(self._prefixed(redis_key), mapping=mapping)

        await pipe.execute()

    async def insert_hash(self, key: str, data: dict):
        mapping = {str(k): orjson.dumps(v).decode() for k, v in data.items()}
        await self.redis.hset(self._prefixed(key), mapping=mapping)

    async def execute_script(self, script: str, numkeys: int, keys_and_args: str):
        items = keys_and_args.split(',')
        prefixed = [self._prefixed(item) if i < numkeys else item for i, item in enumerate(items)]
        return await self.redis.eval(script, numkeys, *prefixed)

    async def zadd_value(self, key: str, value: str, timestamp: datetime.time):
        await self.redis.zadd(self._prefixed(key), {value: timestamp})


redis_client = RedisClient.get_instance()
