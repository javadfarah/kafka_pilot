# Kafka Pilot
<img width="400" height="300" alt="kafka_pilot" src="https://github.com/user-attachments/assets/0b24ef00-8d25-4cb4-b4c5-fd07c1b9bf67" />

**Kafka Pilot** is an asynchronous, modular Kafka worker management system built with **FastAPI**, **aiokafka**, and **Redis**.  
It provides a framework for building fault-tolerant Kafka consumers with retry logic, DLQ support, lag policies, self-healing worker supervision, and a full-featured web admin dashboard.

---

## 🚀 Features

- **Async Kafka consumers** powered by `aiokafka`
- **Worker Supervisor**: local task monitoring, crash detection, and automatic restart with exponential backoff
- **Redis-backed state tracking**: lease-based worker heartbeats, non-blocking scans, and connection keep-alive
- **Redis Sentinel High Availability & Key Prefixing**: automatic failover and Redis namespace isolation
- **Configurable lag policies**: Seek-to-End and Start-of-Day with offset commits
- **Time-based offset seeking**: seek consumers to any specific ISO datetime with processor reset hooks
- **Retry & DLQ** (Dead Letter Queue) handling
- **Dynamic processor loading** – one or more workers per topic
- **MessagePublisher Interface**: extensible abstraction for publishing outgoing data
- **Modern Web Admin Panel**: real-time metrics, worker logs viewer, health probes, and manual actions
- **Kubernetes-ready health checks**: `/api/admin/health` reflecting Redis, Kafka consumers, and worker states
- **Clean modular architecture** following SOLID principles

---

## 🧩 Project Structure

```
kafka_pilot/
  ├── config.py              # Settings via Pydantic & environment variables
  ├── main.py                # FastAPI entrypoint & lifecycle
  ├── worker.py              # Kafka worker manager & consumer loop
  ├── supervisor.py          # Background worker supervisor & auto-healing
  ├── web_admin.py           # Admin API routes & dashboard
  ├── publisher/             # Publisher abstractions (MessagePublisher)
  ├── processors/            # Topic processors & MessageProcessor base
  ├── lag_policies/          # Lag handling policies (SeekToEnd, StartOfDay)
  ├── schema/                # TopicConf schema & validation
  ├── db/
  │   └── redis.py           # Redis client with Sentinel & lease management
  ├── templates/             # Jinja2 dashboard templates & static assets
  └── utils/                 # Logging & dynamic module discovery
```

---

## ⚙️ Requirements

- **Python** ≥ 3.10  
- **Kafka** cluster (tested with Apache Kafka ≥ 3.x)
- **Redis** instance or Sentinel cluster (for state and offset tracking)

---

## 🛠 Installation

```bash
git clone https://github.com/javadfarah/kafka_pilot.git
cd kafka_pilot
pip install -r requirements.txt
```

---

## 🔧 Configuration

Kafka Pilot uses environment variables or a `.env` file for configuration.

Example `.env` file:

```env
# Environment
ENVIRONMENT=dev

# Redis standalone configuration
KFP__REDIS_HOST=localhost
KFP__REDIS_PORT=6379
KFP__REDIS_DB_NUMBER=0
KFP__REDIS_PASSWORD=
KFP__REDIS_KEY_PREFIX=kfp_

# Optional Redis Sentinel configuration
# KFP__REDIS_USE_SENTINEL=true
# KFP__REDIS_SENTINEL_MASTER_NAME=mymaster
# KFP__REDIS_SENTINEL_HOSTS=sentinel1:26379,sentinel2:26379

# Kafka configuration
KFP__KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KFP__KAFKA_SECURITY_PROTOCOL=PLAINTEXT
KFP__KAFKA_GROUP_ID=kafka-pilot-group

# Processor directory
KFP__KAFKA_CONF_DIRECTORY=./processors
```

---

## ▶️ Run the Application

### Using Uvicorn (recommended)
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

### Or directly
```bash
python main.py
```

---

## 🌐 Web Admin Panel

Once the service is running, the admin panel is available at:

```
http://localhost:8000/admin
```

It provides:
- Live worker status (running, restarting, degraded, stopped)
- Total lag per topic and partition assignments
- Offset seek controls (by timestamp/timezone)
- Stored worker log viewer with search and pagination
- Health check modal with Kubernetes readiness inspection

---

## 🧠 Creating a Processor

Each topic processor should inherit from `MessageProcessor` and define its configuration via `TopicConf`.

Example: `processors/my_processor.py`

```python
from kafka_pilot import MessageProcessor, TopicConf

class MyProcessor(MessageProcessor):
    topic_conf = TopicConf(
        topics="my-topic",
        max_retries=5,
        backoff_ms=500,
        backoff_multiplier=2,
        processing_timeout_s=30,
        lag_policy="SeekToTheEndLagPolicy",
        acceptable_lag=10000,
    )

    async def process_message(self, message):
        print(f"Processing message: {message.value}")
        # Your custom business logic here

    async def on_seek(self, target_dt=None):
        """Optional hook called when consumer offsets are seeked."""
        print(f"Seeked to {target_dt}, resetting in-memory buffers")
```

Simply place the file inside your configured processors directory (`./processors`).

---

## ⚙️ TopicConf Parameters

| Parameter | Description |
|------------|--------------|
| `topics` | Kafka topic name(s) comma-separated |
| `max_retries` | Max retry attempts before sending to DLQ |
| `backoff_ms` | Initial backoff delay in milliseconds |
| `backoff_multiplier` | Exponential backoff multiplier |
| `processing_timeout_s` | Message processing timeout in seconds |
| `lag_policy` | Lag management strategy (`SeekToTheEndLagPolicy`, `StartOfDayLagPolicy`) |
| `acceptable_lag` | Threshold before triggering lag handling |
| `auto_commit` | Whether offsets are auto-committed |
| `kafka_bootstrap_servers`, `kafka_group_id`, etc. | Kafka client connection overrides |

---

## 🩺 Health Check & Kubernetes Probes

A comprehensive health endpoint is available for monitoring and readiness/liveness probes:

```bash
GET /api/admin/health
```

Response:
```json
{
  "status": "healthy",
  "status_code": 200,
  "redis": {
    "status": "connected",
    "error": null
  },
  "kafka": {
    "status": "connected",
    "active_workers": 4,
    "errors": []
  },
  "supervisor": {
    "status": "healthy",
    "supervisor_running": true,
    "total_workers": 4,
    "active_workers": 4,
    "restarting_workers": 0,
    "degraded_workers": 0,
    "failed_workers": 0
  }
}
```

---

## 🧱 Design Principles

Kafka Pilot follows:
- **SOLID principles**
- **Dependency inversion** (easy to extend)
- **Separation of concerns** (producers, consumers, retry, DLQ, lag policies)
- **Self-healing** (supervisors automatically recover failed consumer tasks)
- **Extensibility** through pluggable processor modules

---

## 📄 License

**MIT License** — You are free to use, modify, and distribute this project with attribution.

---

## 🤝 Contributing

Contributions are welcome!  
Feel free to open issues or pull requests to improve the framework.

---

## 📬 Contact

**Author:** MJ Farahnak  
GitHub: [@javadfarah](https://github.com/javadfarah)
