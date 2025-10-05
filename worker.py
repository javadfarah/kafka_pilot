# worker.py
"""Kafka worker implementation with Exactly-Once Semantics."""
import asyncio
import uuid
from typing import Optional

from aiokafka import AIOKafkaConsumer

from kafka_pilot.config import get_conf
from kafka_pilot.db import redis_client
from kafka_pilot.lag_policies import LagPolicyInterface
from kafka_pilot.processors import MessageProcessor
from kafka_pilot.utils import logger


class KafkaWorker:
    """Worker for processing messages from a single Kafka topic with EOS."""

    def __init__(self, processor: MessageProcessor, topic: str):
        """Initialize the worker with configurations and Redis client."""
        self.topic_config = processor.topic_conf
        self.kafka_config = get_conf()
        self.redis_client = redis_client
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False
        self.topic = topic
        self.processor = processor
        self.processor.batch_count = 100
        self.worker_id = str(uuid.uuid4())
        self.partitions = set()
        self.lag_policy = self.topic_config.lag_policy

    async def start(self):
        """Start the worker and begin consuming messages."""
        self.running = True
        await self.redis_client.set_worker_status(self.topic, self.worker_id, "running", self.partitions)
        while self.running:
            await self._initialize_kafka()
            await self._consume_loop()

    async def stop(self):
        """Gracefully stop the worker."""
        self.running = False
        await self.redis_client.set_worker_status(self.topic, self.worker_id, "stopped", self.partitions)
        if self.consumer:
            await self.consumer.stop()

        logger.info(f"Worker stopped for topic: {self.topic}")

    async def _initialize_kafka(self):
        """Initialize Kafka consumer"""

        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.topic_config.kafka_bootstrap_servers,
            group_id=self.topic_config.kafka_group_id,
            enable_auto_commit=self.topic_config.auto_commit,
            isolation_level=self.topic_config.isolation_level,
            security_protocol=self.topic_config.security_protocol,
            sasl_mechanism=self.topic_config.sasl_mechanism,
            sasl_plain_password=self.topic_config.kafka_password,
            sasl_plain_username=self.topic_config.kafka_username, max_poll_interval_ms=600000
        )

        await self.consumer.start()
        await self._get_lag_policy_class()
        if self.lag_policy:
            await self._check_lag_and_seek_if_needed()
        self.partitions = await self._get_assigned_partitions()
        await self.redis_client.set_worker_status(self.topic, self.worker_id, "running", self.partitions)

    async def _get_lag_policy_class(self):
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
            retries = 0
            while retries <= self.topic_config.max_retries:
                try:
                    # Process message within transaction
                    await self.processor.process(msg)
                    if self.topic_config.auto_commit is False:
                        await self.consumer.commit()
                    await self.redis_client.increment_processed(self.topic)
                    break
                except Exception as e:
                    retries += 1
                    await self.redis_client.increment_retry(self.topic)
                    await self.redis_client.log_message(self.topic, self.worker_id, f"Error: {e},message: {msg}")
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
                    raise

    async def _send_to_dlq(self, msg):
        """Send message to Dead Letter Queue within a transaction."""
        dlq_topic = f"{self.topic}{self.kafka_config.dlq_topic_suffix}"
        await self.redis_client.increment_dlq(self.topic)
        await self.redis_client.log_message(self.topic, self.worker_id, f"Sent to DLQ: {dlq_topic}")
        logger.info(f"Message sent to DLQ: {dlq_topic}")

    async def _cleanup(self):
        """Clean up Kafka connections."""
        if self.consumer:
            await self.consumer.stop()

        self.consumer = None

    async def _check_lag_and_seek_if_needed(self):
        """Check consumer lag and seek to today's first offset if lag > 200k and committed offset is older."""

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

        # Get committed offsets per partition (one by one)
        committed_offsets = {}
        for tp in partitions:
            committed = await self.consumer.committed(tp)
            committed_offsets[tp] = committed if committed is not None else 0

        # Calculate total lag
        total_lag = sum(
            end_offsets.get(tp, 0) - committed_offsets.get(tp, 0)
            for tp in partitions
        )
        if total_lag <= self.topic_config.acceptable_lag:
            logger.info(f"Total lag is acceptable ({total_lag}). No seek needed.")
            return

        logger.warning(f"High lag detected ({total_lag}). executing lag policy")
        await self.lag_policy(total_lag=total_lag, partitions=partitions, consumer=self.consumer,
                              committed_offsets=committed_offsets).execute()
