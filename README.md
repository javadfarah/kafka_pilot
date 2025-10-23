# Kafka Pilot

**Kafka Pilot** is an asynchronous, modular Kafka worker management system built with **FastAPI**, **aiokafka**, and **Redis**.  
It provides a framework for building fault-tolerant Kafka consumers with retry logic, DLQ support, lag policies, and a simple web admin panel.

---

## 🚀 Features

- **Async Kafka consumers** powered by `aiokafka`
- **Redis-backed** state tracking
- **Configurable lag policies** (e.g., seek-to-beginning, seek-to-end)
- **Retry & DLQ** (Dead Letter Queue) handling
- **Dynamic processor loading** – one processor per topic
- **Web Admin Panel** to monitor topics, workers, and processors
- **Health check endpoints**
- **Clean modular architecture** following SOLID principles

---

## 🧩 Project Structure

```
kafka_pilot/
  ├── config.py
  ├── main.py                # FastAPI entrypoint
  ├── worker.py              # Kafka worker manager
  ├── web_admin.py           # Admin web interface
  ├── processors/            # Topic processors
  ├── lag_policies/          # Lag handling policies
  ├── db/
  │   └── redis.py           # Redis connection wrapper
  └── utils/                 # Helpers & loggers
```

---

## ⚙️ Requirements

- **Python** ≥ 3.10  
- **Kafka** cluster (tested with Apache Kafka ≥ 3.x)
- **Redis** instance (for state and offset tracking)

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

# Redis configuration
KFP__REDIS_HOST=localhost
KFP__REDIS_PORT=6379
KFP__REDIS_DB_NUMBER=0
KFP__REDIS_PASSWORD=

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

It shows worker status, registered processors, topic lag info, and configuration details.

---

## 🧠 Creating a Processor

Each topic processor should inherit from `MessageProcessor` and define its configuration via `TopicConf`.

Example: `processors/my_processor.py`

```python
from processors import MessageProcessor, TopicConf

class MyProcessor(MessageProcessor):
    topic_conf = TopicConf(
        topics="my-topic",
        max_retries=5,
        backoff_ms=500,
        backoff_multiplier=2,
        processing_timeout_s=30,
    )

    async def process_message(self, message):
        print(f"Processing: {message.value}")
        # Your custom logic here
```

Simply place the file inside your configured processors directory (`./processors`).

---

## ⚙️ TopicConf Parameters

| Parameter | Description |
|------------|--------------|
| `topics` | Kafka topic name(s) |
| `max_retries` | Max retry attempts before sending to DLQ |
| `backoff_ms` | Initial backoff delay in milliseconds |
| `backoff_multiplier` | Exponential backoff multiplier |
| `processing_timeout_s` | Message processing timeout in seconds |
| `lag_policy` | Lag management strategy (customizable) |
| `acceptable_lag` | Threshold before triggering lag handling |
| `auto_commit` | Whether offsets are auto-committed |
| `kafka_bootstrap_servers`, `group_id`, etc. | Kafka client options |

---

## 🩺 Health Check

A health endpoint is available for monitoring:

```bash
GET /health
```

Response:
```json
{ "status": "healthy" }
```

---

## 🧱 Design Principles

Kafka Pilot follows:
- **SOLID principles**
- **Dependency inversion** (easy to extend)
- **Separation of concerns** (producers, consumers, retry, DLQ, lag policies)
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
Email: *available on profile*
