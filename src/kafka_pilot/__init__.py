"""Kafka Pilot - Async modular Kafka worker management framework."""
import sys
from pathlib import Path

# Auto-register parent path when running standalone from root repository
_pkg_path = Path(__file__).resolve().parent
if _pkg_path.name == "kafka_pilot" and str(_pkg_path.parent) not in sys.path:
    sys.path.insert(0, str(_pkg_path.parent))

from .processors.processor_interface import MessageProcessor
from .schema.topic_conf import TopicConfig, TopicConfig as TopicConf
from .worker import KafkaWorker
from .supervisor import WorkerSupervisor

__all__ = [
    "MessageProcessor",
    "TopicConfig",
    "TopicConf",
    "KafkaWorker",
    "WorkerSupervisor",
]
