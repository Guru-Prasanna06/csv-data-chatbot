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


def test_chat_valid_question_with_data():
    """A question that maps to a known dynamic column + value must return a grounded, real answer."""
    schema = {"properties": ["group"], "sample_values": {"group": ["Billing", "Engineering"]}}
    with patch("app.services.neo4j_service.neo4j_service.check_health", new=AsyncMock(return_value=True)), \
         patch("app.api.v1.endpoints.chat.chat_service.get_graph_schema", new=AsyncMock(return_value=schema)), \
         patch("app.services.neo4j_service.neo4j_service.execute_query", new=AsyncMock(return_value=[{"count": 128}])) as mock_exec:
        response = client.post("/chat", json={"question": "How many rows belong to the Billing group?"})
        assert response.status_code == 200
        data = response.json()
        assert data["grounded"] is True
        assert data["result"] == [{"count": 128}]
        assert "128" in data["answer"]
        assert "group" in data["cypher"]
        # value must be passed as a parameter, never string-interpolated into the query
        called_params = mock_exec.call_args.args[1] if len(mock_exec.call_args.args) > 1 else mock_exec.call_args.kwargs.get("parameters")
        assert called_params == {"value": "Billing"}


def test_chat_no_matching_data():
    """A query that legitimately executes but finds nothing must be marked ungrounded."""
    schema = {"properties": ["group"], "sample_values": {"group": ["Billing", "Engineering"]}}
    with patch("app.services.neo4j_service.neo4j_service.check_health", new=AsyncMock(return_value=True)), \
         patch("app.api.v1.endpoints.chat.chat_service.get_graph_schema", new=AsyncMock(return_value=schema)), \
         patch("app.services.neo4j_service.neo4j_service.execute_query", new=AsyncMock(return_value=[{"count": 0}])):
        response = client.post("/chat", json={"question": "How many rows belong to the Billing group?"})
        assert response.status_code == 200
        data = response.json()
        assert data["grounded"] is False
        assert data["result"] == [{"count": 0}]


def test_chat_empty_database():
    """No CSV has been loaded yet (no properties discovered) - must be honest, never fabricate."""
    schema = {"properties": [], "sample_values": {}}
    with patch("app.services.neo4j_service.neo4j_service.check_health", new=AsyncMock(return_value=True)), \
         patch("app.api.v1.endpoints.chat.chat_service.get_graph_schema", new=AsyncMock(return_value=schema)), \
         patch("app.services.neo4j_service.neo4j_service.execute_query", new=AsyncMock(return_value=[{"count": 0}])):
        response = client.post("/chat", json={"question": "How many rows are there in total?"})
        assert response.status_code == 200
        data = response.json()
        assert data["grounded"] is False


def test_chat_empty_question():
    """An empty/whitespace-only question must be rejected without touching Neo4j."""
    with patch("app.services.neo4j_service.neo4j_service.check_health", new=AsyncMock(return_value=True)) as mock_health:
        response = client.post("/chat", json={"question": "   "})
        assert response.status_code == 200
        data = response.json()
        assert data["grounded"] is False
        assert data["cypher"] == ""
        assert data["result"] == []
        mock_health.assert_not_called()


def test_chat_dangerous_cypher_attempt():
    """If the LLM path proposes a write/destructive query, it must be blocked and never executed."""
    schema = {"properties": ["group"], "sample_values": {"group": ["Billing"]}}
    dangerous_query = "MATCH (r:Row) DETACH DELETE r"

    with patch("app.services.neo4j_service.neo4j_service.check_health", new=AsyncMock(return_value=True)), \
         patch("app.api.v1.endpoints.chat.chat_service.get_graph_schema", new=AsyncMock(return_value=schema)), \
         patch("app.api.v1.endpoints.chat.get_settings") as mock_settings, \
         patch("app.api.v1.endpoints.chat.chat_service.generate_cypher_llm", new=AsyncMock(return_value=dangerous_query)), \
         patch("app.services.neo4j_service.neo4j_service.execute_query", new=AsyncMock(return_value=[{"count": 1}])) as mock_exec:
        mock_settings.return_value.OPENAI_API_KEY = "fake-key-for-test"
        response = client.post("/chat", json={"question": "Delete all rows in Billing"})
        assert response.status_code == 200
        # The dangerous query must never reach Neo4j; only the safe fallback query may execute.
        for call in mock_exec.call_args_list:
            executed_query = call.args[0] if call.args else call.kwargs.get("query")
            assert "DELETE" not in executed_query.upper()
            assert "DETACH" not in executed_query.upper()


@pytest.mark.parametrize(
    "dangerous_query",
    [
        "MATCH (r:Row) DETACH DELETE r",
        "CREATE (r:Row {x: 1}) RETURN r",
        "MATCH (r:Row) SET r.x = 1 RETURN r",
        "MATCH (r:Row) REMOVE r.x RETURN r",
        "MATCH (d:Dataset) DELETE d",
        "DROP INDEX ON :Row(x)",
        "LOAD CSV FROM 'file:///etc/passwd' AS row RETURN row",
        "MATCH (r:Row) RETURN r; MATCH (d:Dataset) DELETE d",
        "CALL apoc.periodic.iterate('MATCH (r) RETURN r', 'DELETE r', {})",
        "",
    ],
)
def test_validate_read_only_cypher_rejects_dangerous_queries(dangerous_query):
    from app.services import chat_service

    with pytest.raises(chat_service.UnsafeCypherError):
        chat_service.validate_read_only_cypher(dangerous_query)


def test_validate_read_only_cypher_accepts_safe_query():
    from app.services import chat_service

    # Must not raise
    chat_service.validate_read_only_cypher("MATCH (r:Row) WHERE r.group = $value RETURN count(r) AS count")


def test_chat_neo4j_unavailable():
    """If Neo4j itself cannot be reached, the chatbot must say so honestly rather than guessing."""
    with patch("app.services.neo4j_service.neo4j_service.check_health", new=AsyncMock(return_value=False)):
        response = client.post("/chat", json={"question": "How many rows are there?"})
        assert response.status_code == 200
        data = response.json()
        assert data["grounded"] is False
        assert data["cypher"] == ""
        assert data["result"] == []


def test_chat_dynamic_property_question():
    """The chatbot must work against arbitrary/dynamic CSV columns, not a hard-coded dataset."""
    schema = {"properties": ["region"], "sample_values": {"region": ["North", "South"]}}
    with patch("app.services.neo4j_service.neo4j_service.check_health", new=AsyncMock(return_value=True)), \
         patch("app.api.v1.endpoints.chat.chat_service.get_graph_schema", new=AsyncMock(return_value=schema)), \
         patch("app.services.neo4j_service.neo4j_service.execute_query", new=AsyncMock(return_value=[{"count": 42}])):
        response = client.post("/chat", json={"question": "How many rows have region North?"})
        assert response.status_code == 200
        data = response.json()
        assert data["grounded"] is True
        assert "region" in data["cypher"]
        assert data["result"] == [{"count": 42}]
