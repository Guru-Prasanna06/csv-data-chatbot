import io
from unittest.mock import AsyncMock, patch
import pytest
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_health_endpoint_real_offline():
    """
    Test real connectivity when services are genuinely offline.
    Verifies that kafka_connected and neo4j_connected are False,
    and status is NOT 'ok'.
    """
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "degraded"
    assert data["kafka_connected"] is False
    assert data["neo4j_connected"] is False


def test_health_endpoint_kafka_only_up():
    """If Kafka is connected but Neo4j is down, status must not be 'ok'."""
    with patch("app.services.kafka_producer.kafka_service.check_health", new=AsyncMock(return_value=True)), \
         patch("app.services.neo4j_service.neo4j_service.check_health", new=AsyncMock(return_value=False)):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "degraded"
        assert data["kafka_connected"] is True
        assert data["neo4j_connected"] is False


def test_health_endpoint_neo4j_only_up():
    """If Neo4j is connected but Kafka is down, status must not be 'ok'."""
    with patch("app.services.kafka_producer.kafka_service.check_health", new=AsyncMock(return_value=False)), \
         patch("app.services.neo4j_service.neo4j_service.check_health", new=AsyncMock(return_value=True)):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "degraded"
        assert data["kafka_connected"] is False
        assert data["neo4j_connected"] is True


def test_health_endpoint_all_services_up():
    """Only when both Kafka and Neo4j are connected should status be 'ok'."""
    with patch("app.services.kafka_producer.kafka_service.check_health", new=AsyncMock(return_value=True)), \
         patch("app.services.neo4j_service.neo4j_service.check_health", new=AsyncMock(return_value=True)):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["kafka_connected"] is True
        assert data["neo4j_connected"] is True




def test_status_endpoint_not_found():
    """Verify that an unknown job_id returns HTTP 404."""
    response = client.get("/status?job_id=unknown_job_999")
    assert response.status_code == 404
    data = response.json()
    assert "detail" in data
    assert "not found" in data["detail"].lower()


def test_status_endpoint_full_lifecycle():
    """
    Verify full job status lifecycle:
    queued -> loading (with real row counts) -> complete.
    """
    from app.services.job_tracker import job_tracker

    # 1. Create a job
    job_id = "b3f1"
    job_tracker.create_job(job_id=job_id, rows_total=1000)

    # Initial status check
    resp = client.get(f"/status?job_id={job_id}")
    assert resp.status_code == 200
    assert resp.json() == {
        "job_id": "b3f1",
        "status": "queued",
        "rows_total": 1000,
        "rows_loaded": 0,
        "rows_failed": 0,
    }

    # 2. Update progress (e.g. reported by Neo4j / loader pipeline)
    update_payload = {
        "status": "loading",
        "rows_loaded": 640,
        "rows_failed": 3,
    }
    update_resp = client.post(f"/status/update?job_id={job_id}", json=update_payload)
    assert update_resp.status_code == 200
    assert update_resp.json() == {
        "job_id": "b3f1",
        "status": "loading",
        "rows_total": 1000,
        "rows_loaded": 640,
        "rows_failed": 3,
    }

    # 3. Verify GET /status returns the updated progress
    get_resp = client.get(f"/status?job_id={job_id}")
    assert get_resp.status_code == 200
    assert get_resp.json() == {
        "job_id": "b3f1",
        "status": "loading",
        "rows_total": 1000,
        "rows_loaded": 640,
        "rows_failed": 3,
    }

    # 4. Mark job complete
    complete_resp = client.post(
        f"/status/update?job_id={job_id}",
        json={"status": "complete", "rows_loaded": 997, "rows_failed": 3},
    )
    assert complete_resp.status_code == 200
    assert complete_resp.json()["status"] == "complete"
    assert complete_resp.json()["rows_loaded"] == 997
    assert complete_resp.json()["rows_failed"] == 3



def test_ingest_small_csv_and_kafka_production():
    """
    Test uploading a small CSV with dynamic columns.
    Verify HTTP 202, rows_received, job tracking, and Kafka messages format.
    """
    published_messages = []

    async def mock_publish(messages, topic=None):
        published_messages.extend(messages)
        return len(messages)

    with patch("app.services.kafka_producer.kafka_service.check_health", new=AsyncMock(return_value=True)), \
         patch("app.services.kafka_producer.kafka_service.publish_row_messages", side_effect=mock_publish):
        
        csv_content = b"customer_id,name,group\n101,Ravi,Billing\n102,Sara,Engineering\n"
        files = {"file": ("customers_001.csv", io.BytesIO(csv_content), "text/csv")}
        
        response = client.post("/ingest", files=files)
        assert response.status_code == 202
        data = response.json()
        assert "job_id" in data
        assert data["rows_received"] == 2
        assert data["status"] == "queued"

        # Verify Job Tracker recorded the job
        job_id = data["job_id"]
        status_resp = client.get(f"/status?job_id={job_id}")
        assert status_resp.status_code == 200
        assert status_resp.json()["rows_total"] == 2

        # Verify Kafka messages produced
        assert len(published_messages) == 2

        # Message 1
        msg1 = published_messages[0]
        assert msg1["job_id"] == job_id
        assert msg1["dataset_id"] == f"customers_001_{job_id}"
        assert msg1["row_index"] == 1
        assert msg1["data"] == {
            "customer_id": "101",
            "name": "Ravi",
            "group": "Billing",
        }

        # Message 2
        msg2 = published_messages[1]
        assert msg2["job_id"] == job_id
        assert msg2["dataset_id"] == f"customers_001_{job_id}"
        assert msg2["row_index"] == 2
        assert msg2["data"] == {
            "customer_id": "102",
            "name": "Sara",
            "group": "Engineering",
        }


def test_ingest_empty_csv():
    """Test empty CSV (0 bytes) returns HTTP 202 with 0 rows received."""
    files = {"file": ("empty.csv", io.BytesIO(b""), "text/csv")}
    response = client.post("/ingest", files=files)
    assert response.status_code == 202
    data = response.json()
    assert data["rows_received"] == 0
    assert data["status"] == "queued"


def test_ingest_header_only_csv():
    """Test header-only CSV with no data rows returns HTTP 202 with 0 rows received."""
    csv_content = b"colA,colB,colC\n"
    files = {"file": ("header_only.csv", io.BytesIO(csv_content), "text/csv")}
    response = client.post("/ingest", files=files)
    assert response.status_code == 202
    data = response.json()
    assert data["rows_received"] == 0
    assert data["status"] == "queued"


def test_ingest_invalid_file_extension():
    """Test invalid non-csv file extension is rejected with HTTP 400."""
    files = {"file": ("report.pdf", io.BytesIO(b"%PDF-1.4 test binary"), "application/pdf")}
    response = client.post("/ingest", files=files)
    assert response.status_code == 400
    data = response.json()
    assert "detail" in data



def test_chat_endpoint():
    payload = {"question": "How many rows belong to the Billing group?"}
    response = client.post("/chat", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert "answer" in data
    assert "cypher" in data
    assert "result" in data
    assert "grounded" in data
    assert isinstance(data["result"], list)
    assert isinstance(data["grounded"], bool)
