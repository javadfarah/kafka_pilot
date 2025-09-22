# Kafka Pilot

Kafka Pilot is an asynchronous FastAPI-based application for managing Kafka consumers.  
It provides Redis-backed tracking, configurable lag policies, retries, a Dead Letter Queue (DLQ) mechanism, and a built‑in web admin panel.  

It’s designed to help you easily manage scalable, exactly‑once Kafka message processing with minimal boilerplate.

---

## ✨ Features

- **Async Kafka Consumers** — Optimized for high throughput and low latency.
- **Redis Integration** — Tracks offsets, states, and metadata centrally.
- **Lag Policies** — Start‑of‑day offsets, seek‑to‑end, and custom strategies.
- **Retries & DLQ** — Automatic error handling with Dead Letter Queue topics.
- **Automatic Processor Registration** — One processor per topic with dynamic loading.
- **Web Admin Panel** — Monitor workers and topics in real‑time via dashboard.
- **Graceful Lifecycle** — Clean startup/shutdown with health check endpoint.

---

## 📦 Installation

```bash
git clone https://github.com/<your-username>/kafka-pilot.git
cd kafka-pilot
pip install -r requirements.txt
```

Ensure you have:
- **Python** >= 3.10
- **Kafka** cluster accessible
- **Redis** running

---

## ⚙️ Configuration

Kafka Pilot uses environment variables (via `.env` file) to configure Kafka and Redis.

Example `.env`:

```env
ENVIRONMENT=dev
KFP__REDIS_HOST=localhost
KFP__REDIS_PORT=6379
KFP__REDIS_DB_NUMBER=0
KFP__REDIS_PASSWORD=
KFP__KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KFP__KAFKA_GROUP_ID=my-consumer-group
KFP__KAFKA_CONF_DIRECTORY=./processors
```

- **KFP__KAFKA_CONF_DIRECTORY** should point to a directory containing your topic processor classes.

---

## 🚀 Running Kafka Pilot

### With Uvicorn
```bash
python -m kafka_pilot.main
```

or

```bash
uvicorn kafka_pilot.main:app --host 0.0.0.0 --port 8000
```

---

## 🌐 Web Admin

Once running, open:

```
http://localhost:8000/admin
```

There you can monitor:
- Active workers
- Assigned topics
- Processor classes
- Consumer status

---

## 📂 Project Structure

```
kafka_pilot/
  ├── config.py               # Settings and .env parsing
  ├── main.py                 # FastAPI app entrypoint
  ├── worker.py               # KafkaWorker class
  ├── web_admin.py            # Web interface setup
  ├── processors/             # Your Kafka message processors
  ├── lag_policies/           # Seek/start logic classes
  ├── db/redis.py             # Redis client
  └── utils/json_logger.py    # JSON log formatter
```

---

## 🛠 Creating a Processor

Processors must inherit from `MessageProcessor` and define:

- `topic_conf` — topics and settings
- `process_message()` — your Kafka message handling logic

```python
from kafka_pilot.processors import MessageProcessor

class MyProcessor(MessageProcessor):
    topic_conf = TopicConf(topics="my-topic")

    async def process_message(self, message):
        print(f"Got: {message.value}")
```

Place your processor file in the configured directory.

---

## 🩺 Health Check

Kafka Pilot exposes `/health`:

```
GET /health
{
  "status": "healthy"
}
```

---

## 📜 License

MIT License — feel free to use, modify, and share.

---
