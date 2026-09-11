# CSV Knowledge Graph Backend

Backend API and Kafka Producer service for the CSV → FastAPI → Kafka → Neo4j → Chatbot pipeline.

## Features & Endpoints

- **`GET /health`**: Genuine reachability health check for Kafka and Neo4j. Returns `"status": "ok"` only when both services are connected.
- **`POST /ingest`**: Dynamic CSV multipart upload. Generates a `job_id`, counts rows, and queues messages to Kafka.
- **`GET /status?job_id=<id>`**: Ingestion progress tracker (`queued`, `loading`, `complete`, `failed`).
- **`POST /chat`**: Natural language query endpoint to query the Neo4j Knowledge Graph.
- Interactive API Docs available at `http://localhost:8000/docs`.

---

## Project Structure

```
.
├── app/
│   ├── api/
│   │   └── v1/
│   │       ├── endpoints/
│   │       │   ├── chat.py       # POST /chat
│   │       │   ├── health.py     # GET /health
│   │       │   ├── ingest.py     # POST /ingest
│   │       │   └── status.py     # GET /status
│   │       └── router.py
│   ├── core/
│   │   └── config.py             # Pydantic Settings & environment config
│   ├── schemas/
│   │   ├── chat.py
│   │   ├── health.py
│   │   ├── ingest.py
│   │   ├── kafka.py
│   │   └── status.py
│   ├── services/
│   │   ├── job_tracker.py        # In-memory / persistent job tracker
│   │   ├── kafka_producer.py     # Kafka producer & healthcheck
│   │   └── neo4j_service.py      # Neo4j driver & healthcheck
│   └── main.py                   # FastAPI app factory
├── main.py                       # Application entrypoint
├── requirements.txt              # Dependencies
├── .env.example                  # Environment configuration template
└── README.md
```

---

## Getting Started

### 1. Prerequisites
- Python 3.11+
- Virtualenv

### 2. Environment Setup

```bash
# From the repo root, enter the backend folder
cd backend

# Create virtual environment
python3.11 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create .env file
cp .env.example .env
```

### 3. Run the Backend Server

```bash
# Using uvicorn directly
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# Or run main.py
python main.py
```

### 4. Verify Endpoints

#### Health Check
```bash
curl http://localhost:8000/health
```

#### Upload / Ingest CSV
```bash
curl -X POST "http://localhost:8000/ingest" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@sample.csv"
```

#### Check Job Status
```bash
curl "http://localhost:8000/status?job_id=YOUR_JOB_ID"
```

#### Chat Query
```bash
curl -X POST "http://localhost:8000/chat" \
  -H "Content-Type: application/json" \
  -d '{"question": "How many rows belong to the Billing group?"}'
```

---

## Docker

```bash
# From the backend/ folder
docker build -t csv-backend:1.0.0 .
docker run -d --name csv-chatbot-backend -p 8000:8000 --env-file .env csv-backend:1.0.0
```

Runs as a non-root user (`appuser`), exposes port 8000, and includes a container healthcheck against `/health`.
