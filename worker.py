# worker.py
"""Kafka worker implementation with Exactly-Once Semantics."""
import asyncio
from datetime import datetime, timezone
import time
import uuid
from typing import Any, Optional
import orjson
from aiokafka import AIOKafkaConsumer, TopicPartition
from aiokafka.errors import CommitFailedError

from kafka_pilot.config import get_conf
from kafka_pilot.db import redis_client
from kafka_pilot.db.redis import WORKER_HEARTBEAT_SECONDS

LAG_PERSIST_INTERVAL_SECONDS = 10
from kafka_pilot.lag_policies import LagPolicyInterface
from kafka_pilot.processors import MessageProcessor
from kafka_pilot.utils import logger
#TODO: outbox transaction

RESTART_BACKOFF_SECONDS = 5


def decode_value(value: Optional[bytes]) -> Any:
    """Deserialize a Kafka message value from JSON bytes.

    A single undecodable record must not kill the consumer. Callers already
    ignore a non-dict payload and then commit past it.
    """
    if not value:
        return None
    try:
        return orjson.loads(value)
    except orjson.JSONDecodeError:
        logger.warning("Kafka message value is not valid JSON; consumer will skip it")
        return None


class KafkaWorker:
    """Worker for processing messages from a single Kafka topic with EOS."""

    def __init__(self, processor: MessageProcessor, topic: str):
        """Initialize the worker with configurations and Redis client."""
        self.topic_config = processor.topic_conf
        self.kafka_config = get_conf()
        self.redis_client = redis_client
        self.consumer: AIOKafkaConsumer
        self.running = False
        self.topic = topic
        self.processor = processor
        self.worker_id = str(uuid.uuid4())
        self.partitions = set()
        self.lag_policy = self.topic_config.lag_policy
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._stopped_recorded = False
        self._last_lag_persist_at = 0.0
        self.last_error: Optional[str] = None
        self.last_error_at: Optional[datetime] = None
        self.consecutive_errors: int = 0
        self.total_errors: int = 0
        self.last_message_at: Optional[datetime] = None
        self.kafka_connected: bool = False

    def get_health(self) -> dict:
        """Return live health and error metrics for this worker."""
        consumer = getattr(self, "consumer", None)
        consumer_state = "uninitialized"
        partitions = list(self.partitions) if self.partitions else []

        if consumer is not None:
            if getattr(consumer, "_closed", False):
                consumer_state = "closed"
            else:
                assignment_fn = getattr(consumer, "assignment", None)
                if callable(assignment_fn):
                    res = assignment_fn()
                    if isinstance(res, (set, list, tuple)):
                        partitions = [tp.partition for tp in res]
                        consumer_state = "assigned" if partitions else "unassigned"

        return {
            "worker_id": self.worker_id,
            "topic": self.topic,
            "running": self.running,
            "consumer_state": consumer_state,
            "partitions": partitions,
            "consecutive_errors": self.consecutive_errors,
            "total_errors": self.total_errors,
            "last_error": self.last_error,
            "last_error_at": self.last_error_at.isoformat() if self.last_error_at else None,
            "last_message_at": self.last_message_at.isoformat() if self.last_message_at else None,
        }

    def _consumer_group_id(self) -> str:
        """One group per topic.

        Workers of the same topic still share a group so partitions are split.
        Workers of different topics must not: they subscribe to different topics,
        and one shared group rebalances all of them together.
        """
        base = (self.topic_config.kafka_group_id or "kafka-pilot").strip()
        suffix = f".{self.topic}"
        if base.endswith(suffix):
            return base
        return f"{base}{suffix}"

    async def start(self):
        """Start the worker and keep it consuming until stop() is called or an error occurs."""
        self.running = True
        self._stopped_recorded = False
        await self.redis_client.set_worker_status(self.topic, self.worker_id, "running", self.partitions)
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            while self.running:
                await self._initialize_kafka()
                await self._consume_loop()
        finally:
            self.running = False
            await self._close_consumer()
            await self._record_stopped()

    async def stop(self):
        """Gracefully stop the worker."""
        self.running = False
        await self._close_consumer()
        await self._record_stopped()

        logger.info(f"Worker stopped for topic: {self.topic}")

    async def _record_stopped(self):
        """Write the terminal status last so no in-flight write revives it.

        Both stop() and the consume loop unwinding call this, and the loop can
        unwind after shutdown has already closed Redis, so only the first
        caller writes.
        """
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None
        if self._stopped_recorded:
            return
        try:
            await self.redis_client.set_worker_status(
                self.topic, self.worker_id, "stopped", self.partitions
            )
            try:
                await self.redis_client.delete_topic_lag(self.topic, self.worker_id)
            except Exception:
                pass
            self._stopped_recorded = True
        except Exception as e:
            logger.error(f"Failed to mark {self.topic}:{self.worker_id} stopped: {e}")

    async def _heartbeat_loop(self):
        """Renew the Redis lease so a killed worker stops looking alive."""
        try:
            while self.running:
                await asyncio.sleep(WORKER_HEARTBEAT_SECONDS)
                if not self.running:
                    break
                try:
                    await self.redis_client.set_worker_status(
                        self.topic, self.worker_id, "running", self.partitions
                    )
                    consumer = getattr(self, "consumer", None)
                    if consumer is not None:
                        try:
                            await self._maybe_persist_lag(force=True)
                        except Exception as e:
                            self.last_error = f"Kafka lag refresh failed: {e}"
                            self.last_error_at = datetime.now(timezone.utc)
                            self.consecutive_errors += 1
                            self.total_errors += 1
                            logger.warning(
                                f"Failed to persist lag for {self.topic}:{self.worker_id}: {e}"
                            )
                except Exception as e:
                    self.last_error = f"Status refresh failed: {e}"
                    self.last_error_at = datetime.now(timezone.utc)
                    self.consecutive_errors += 1
                    self.total_errors += 1
                    logger.warning(
                        f"Failed to refresh status for {self.topic}:{self.worker_id}: {e}"
                    )
        except asyncio.CancelledError:
            pass

    async def _initialize_kafka(self):
        """Initialize Kafka consumer"""

        group_id = self._consumer_group_id()
        logger.info(f"Starting consumer for topic {self.topic} in group {group_id}")
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.topic_config.kafka_bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=self.topic_config.auto_commit,
            isolation_level=self.topic_config.isolation_level,
            security_protocol=self.topic_config.security_protocol,
            sasl_mechanism=self.topic_config.sasl_mechanism,
            sasl_plain_password=self.topic_config.kafka_password,
            sasl_plain_username=self.topic_config.kafka_username,
            max_poll_interval_ms=600000,
            auto_offset_reset="earliest",
            value_deserializer=decode_value
        )

        try:
            await self.consumer.start()
            self.kafka_connected = True
            await self._get_lag_policy_class()
            if self.lag_policy:
                await self._check_lag_and_seek_if_needed()
            self.partitions = await self._get_assigned_partitions()
            if self.running:
                await self.redis_client.set_worker_status(self.topic, self.worker_id, "running", self.partitions)
        except Exception as e:
            self.last_error = f"Kafka init failed: {e}"
            self.last_error_at = datetime.now(timezone.utc)
            self.consecutive_errors += 1
            self.total_errors += 1
            self.kafka_connected = False
            raise

    async def _get_lag_policy_class(self):
        if not self.lag_policy or isinstance(self.lag_policy, type):
            return
        lag_policy_class = LagPolicyInterface.registry.get(self.lag_policy, None)
        if lag_policy_class is None:
            all_lag_policies = LagPolicyInterface.registry.keys()
            raise ValueError(f"Lag policy is invalid, possible values are : {all_lag_policies}")
        self.lag_policy = lag_policy_class

    async def _get_assigned_partitions(self):
        """Get assigned partitions for this worker."""
        assignment = self.consumer.assignment()  # Not async
        return {tp.partition for tp in assignment}

    async def _consume_loop(self):
        """Main consumption loop with retry and DLQ handling."""
        async for msg in self.consumer:
            if not self.running:
                break
            self.last_message_at = datetime.now(timezone.utc)
            self.consecutive_errors = 0
            self.kafka_connected = True
            try:
                await self._maybe_persist_lag()
            except Exception as e:
                self.last_error = f"Lag persist failed: {e}"
                self.last_error_at = datetime.now(timezone.utc)
                self.consecutive_errors += 1
                self.total_errors += 1
            retries = 0
            while retries <= self.topic_config.max_retries:
                try:
                    # Process message within transaction
                    timeout_s = getattr(self.topic_config, "processing_timeout_s", None)
                    if timeout_s:
                        await asyncio.wait_for(self.processor.process(msg), timeout=timeout_s)
                    else:
                        await self.processor.process(msg)
                    if not self.topic_config.auto_commit:
                        await self.consumer.commit()
                    try:
                        await self.redis_client.increment_processed(self.topic)
                    except Exception as redis_err:
                        logger.warning(f"Failed to increment processed for {self.topic}: {redis_err}")
                    break
                except (asyncio.TimeoutError, TimeoutError):
                    logger.error(
                        f"Processing timeout ({getattr(self.topic_config, 'processing_timeout_s', None)}s) exceeded for {self.topic}"
                    )
                    tp = TopicPartition(msg.topic, getattr(msg, "partition", 0))
                    await self.consumer.commit({tp: msg.offset + 1})
                    break
                except CommitFailedError:
                    logger.warning(
                        f"Offset commit failed after rebalance for {self.topic}; continuing"
                    )
                    break
                except Exception as e:
                    logger.error(str(e), exc_info=True)
                    retries += 1
                    self.last_error = f"Message error: {e}"
                    self.last_error_at = datetime.now(timezone.utc)
                    self.consecutive_errors += 1
                    self.total_errors += 1
                    try:
                        await self.redis_client.increment_retry(self.topic)
                        await self.redis_client.log_message(self.topic, self.worker_id, f"Error: {e},message: {msg}")
                    except Exception as redis_err:
                        logger.warning(f"Failed to record retry telemetry for {self.topic}: {redis_err}")
                    if retries > self.topic_config.max_retries:
                        logger.error(f"Max retries exceeded for message in topic {self.topic}")
                        try:
                            await self.consumer.commit()
                        except Exception as dlq_error:
                            logger.error(f"Failed to send to DLQ: {dlq_error}")
                            raise
                        break
                    backoff = self.topic_config.backoff_ms * (self.topic_config.backoff_multiplier ** (retries - 1))
                    await asyncio.sleep(backoff / 100.0)
                    logger.warning(
                        f"Retry {retries}/{self.topic_config.max_retries} for topic {self.topic}")

    async def _send_to_dlq(self, msg):
        """Send message to Dead Letter Queue within a transaction."""
        dlq_topic = f"{self.topic}{self.kafka_config.dlq_topic_suffix}"
        await self.redis_client.increment_dlq(self.topic)
        await self.redis_client.log_message(self.topic, self.worker_id, f"Sent to DLQ: {dlq_topic}")
        logger.info(f"Message sent to DLQ: {dlq_topic}")

    async def _close_consumer(self):
        """Stop the current consumer so a restart can open a new one."""
        consumer = getattr(self, "consumer", None)
        if consumer is None:
            return
        self.consumer = None
        try:
            await consumer.stop()
        except Exception as e:
            logger.warning(f"Failed to stop consumer for {self.topic}: {e}")

    async def _cleanup(self):
        """Clean up Kafka connections."""
        await self._close_consumer()

    async def _maybe_persist_lag(self, force: bool = False) -> None:
        """Refresh Redis lag at most every LAG_PERSIST_INTERVAL_SECONDS while consuming."""
        now = time.monotonic()
        if not force and now - self._last_lag_persist_at < LAG_PERSIST_INTERVAL_SECONDS:
            return
        consumer = getattr(self, "consumer", None)
        if consumer is None:
            return
        assignment_fn = getattr(consumer, "assignment", None)
        if not callable(assignment_fn):
            return
        res = assignment_fn()
        if not isinstance(res, (set, list, tuple)):
            return
        partitions = list(res)
        if not partitions:
            return
        await self._persist_topic_lag(partitions)

    async def _persist_topic_lag(self, partitions) -> int:
        """Compute lag from end offsets vs current position and store it in Redis."""
        try:
            end_offsets = await self.consumer.end_offsets(partitions)
        except Exception as e:
            self.last_error = f"Kafka fetch offsets failed: {e}"
            self.last_error_at = datetime.now(timezone.utc)
            self.consecutive_errors += 1
            self.total_errors += 1
            logger.warning(f"Failed to persist lag for {self.topic}:{self.worker_id}: {e}")
            return 0

        total_lag = 0
        for tp in partitions:
            try:
                position = await self.consumer.position(tp)
                total_lag += max(0, end_offsets.get(tp, 0) - position)
            except Exception as e:
                self.last_error = f"Kafka position failed: {e}"
                self.last_error_at = datetime.now(timezone.utc)
                self.consecutive_errors += 1
                self.total_errors += 1
                logger.warning(f"Failed to fetch position for {tp}: {e}")
        try:
            await self.redis_client.set_topic_lag(self.topic, self.worker_id, total_lag)
            self._last_lag_persist_at = time.monotonic()
        except Exception as e:
            logger.warning(f"Failed to persist lag for {self.topic}:{self.worker_id}: {e}")
        return total_lag

    async def _check_lag_and_seek_if_needed(self):
        """Check consumer lag and run the configured lag policy when above acceptable_lag."""

        if not self.consumer:
            logger.warning("Consumer not initialized.")
            return

        # Get assigned partitions
        partitions = self.consumer.assignment()
        if not partitions:
            logger.warning("No partitions assigned yet. Skipping lag check.")
            return

        partitions = list(partitions)  # Ensure it's a list
        end_offsets = await self.consumer.end_offsets(partitions)
        beginning_offsets = await self.consumer.beginning_offsets(partitions)

        # Prefer committed offsets. A fresh group has committed=None, but
        # auto_offset_reset=latest already parks position at the high watermark,
        # which would report lag 0. Use log-start so unread backlog is visible
        # and lag policies can still run.
        committed_offsets = {}
        for tp in partitions:
            committed = await self.consumer.committed(tp)
            if committed is not None:
                committed_offsets[tp] = committed
            else:
                committed_offsets[tp] = beginning_offsets.get(tp, 0)

        total_lag = sum(
            max(0, end_offsets.get(tp, 0) - committed_offsets.get(tp, 0))
            for tp in partitions
        )
        try:
            await self.redis_client.set_topic_lag(self.topic, self.worker_id, total_lag)
        except Exception as e:
            logger.warning(f"Failed to persist lag for {self.topic}:{self.worker_id}: {e}")

        if total_lag <= self.topic_config.acceptable_lag:
            logger.info(f"Total lag is acceptable ({total_lag}). Continuing without skip.")
            for tp in partitions:
                target = committed_offsets[tp]
                position = await self.consumer.position(tp)
                if position != target:
                    self.consumer.seek(tp, target)
                    logger.info(
                        f"Resume partition {tp.partition} from {position} to committed offset {target}"
                    )
            return

        logger.warning(f"High lag detected ({total_lag}). executing lag policy")
        await self.lag_policy(total_lag=total_lag, partitions=partitions, consumer=self.consumer,
                              committed_offsets=committed_offsets).execute()
        # Seek moves position but Redis still held pre-seek lag until refresh.
        await self._persist_topic_lag(partitions)

    async def seek_to_time(self, timestamp_ms: int, reset_state: bool = True) -> dict:
        """Seek assigned partitions to the earliest offset at or after timestamp_ms."""
        if not self.consumer:
            return {
                "worker_id": self.worker_id,
                "status": "error",
                "message": "Consumer not initialized",
                "seeked_partitions": {},
            }

        assignment = list(self.consumer.assignment() or [])
        if not assignment:
            return {
                "worker_id": self.worker_id,
                "status": "no_partitions",
                "message": "No partitions assigned to this worker",
                "seeked_partitions": {},
            }

        timestamps = {tp: timestamp_ms for tp in assignment}
        offsets_for_time = await self.consumer.offsets_for_times(timestamps)
        to_commit = {}
        seeked = {}

        for tp in assignment:
            offset_info = offsets_for_time.get(tp)
            if offset_info is None or offset_info.offset < 0:
                logger.warning(
                    f"[{self.topic}:{self.worker_id}] No offset found at or after timestamp {timestamp_ms} for partition {tp.partition}. Keeping current offset."
                )
                continue

            target_offset = offset_info.offset
            self.consumer.seek(tp, target_offset)
            to_commit[tp] = target_offset
            seeked[str(tp.partition)] = target_offset
            logger.info(
                f"[{self.topic}:{self.worker_id}] Seeked partition {tp.partition} to offset {target_offset}"
            )

        if to_commit:
            await self.consumer.commit(to_commit)
            logger.info(
                f"[{self.topic}:{self.worker_id}] Committed seek offsets for {len(to_commit)} partition(s)"
            )

        if reset_state and hasattr(self.processor, "on_seek"):
            target_dt = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
            try:
                await self.processor.on_seek(target_dt)
            except Exception as exc:
                logger.error(f"[{self.topic}:{self.worker_id}] Error in processor.on_seek: {exc}", exc_info=True)

        await self._persist_topic_lag(assignment)

        return {
            "worker_id": self.worker_id,
            "status": "seeked" if seeked else "no_match",
            "seeked_partitions": seeked,
        }
