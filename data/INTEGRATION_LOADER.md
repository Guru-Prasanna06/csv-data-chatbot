# CSV → Kafka → Neo4j Loader Integration Documentation

## 1. Purpose of Loader Lane
The Loader lane is responsible exclusively for consuming structured CSV row events from Apache Kafka, validating message schemas, and loading them into Neo4j in a completely idempotent manner using Cypher `MERGE` queries. It acts as the reliable bridge between upstream event streaming (Kafka) and downstream graph storage (Neo4j).

---

## 2. Architecture
```
[Kafka Topic: csv-rows] 
         │
         ▼ (Manual Offset Poll)
[KafkaLoaderConsumer] ────► [Payload Validator]
         │ (Validated Payload)      │ (Invalid Payload)
         ▼                          ▼
[Neo4jWriter MERGE]         [Log Warning & Commit Offset]
         │ (Success)
         ▼
[Commit Kafka Offset]
         │
         ▼
 Neo4j Graph Database
 (:Dataset)-[:HAS_ROW]->(:Row)
```

---

## 3. Kafka Topic
- **Topic Name**: `csv-rows`
- **Partitioning**: Single partition (single broker KRaft mode).
- **Readiness Handling**: Programmatically checks for topic existence via `KafkaAdminClient` and creates it if absent before entering main polling loop.

---

## 4. Kafka Consumer Group
- **Consumer Group ID**: `csv-neo4j-loader`
- **Auto Commit**: Disabled (`enable_auto_commit=False`).

---

## 5. Kafka Message Contract
The Kafka topic receives JSON-encoded messages produced upstream.

### Mandatory Payload Schema
```json
{
  "dataset_id": "abc123",
  "row_index": 1,
  "row_data": {
    "Customer": "Alice",
    "Order": "O101",
    "Amount": 250
  },
  "filename": "customers.csv",
  "uploaded_at": "2026-09-11T10:00:00Z"
}
```

### Validation Rules
- `dataset_id` (Required, string, non-empty)
- `row_index` (Required, integer, non-boolean)
- `row_data` (Required, JSON object/map, non-empty)
- `filename` (Optional, string)
- `uploaded_at` (Optional, string)

---

## 6. Neo4j Database
- **Host**: `bolt://neo4j:7687`
- **Username**: `neo4j`
- **Password**: `csvgraphdb`
- **Configured Database**: `CSV_Graph_DB`

### Database Availability Verification
During startup, the Loader explicitly performs three checks before marking itself ready:
1. Verifies TCP Bolt connectivity (`driver.verify_connectivity()`).
2. Opens a session specifically against the configured target database (`CSV_Graph_DB`).
3. Executes a test Cypher query (`RETURN 1 AS result`).

---

## 7. Graph Model
Mandatory strict model:
```cypher
(:Dataset {
    id: STRING,
    filename: STRING,
    uploaded_at: STRING
})-[:HAS_ROW]->(:Row {
    dataset_id: STRING,
    row_index: INTEGER,
    ...dynamic CSV columns...
})
```

---

## 8. Idempotency Strategy
To ensure that replaying Kafka events or running duplicate ingestion pipelines produces zero duplicate nodes or relationships, all graph mutations use Cypher `MERGE`. `CREATE` is never used for Row nodes.

---

## 9. MERGE Strategy
```cypher
MERGE (d:Dataset {id: $dataset_id})
ON CREATE SET
    d.filename = $filename,
    d.uploaded_at = $uploaded_at
ON MATCH SET
    d.filename = coalesce($filename, d.filename),
    d.uploaded_at = coalesce($uploaded_at, d.uploaded_at)
MERGE (r:Row {
    dataset_id: $dataset_id,
    row_index: $row_index
})
SET r += $row_data
MERGE (d)-[:HAS_ROW]->(r)
```

---

## 10. Manual Offset Commit Strategy
Kafka offsets are committed **only after** Neo4j `MERGE` successfully succeeds:
```
Kafka Message Received -> Decode & Validate -> Neo4j Write Succeeds -> Commit Kafka Offset
```
If Neo4j write fails due to infrastructure issues, the offset is **not** committed, allowing Kafka to re-deliver the message upon recovery.

---

## 11. Retry / Backoff Strategy
- Both Kafka and Neo4j connections implement exponential backoff retry during startup (initial delay: 2.0s, multiplier: 1.5x, max backoff: 30.0s, max attempts: 30).
- Prevents container crashes during initial container startup sequencing.

---

## 12. Permanent vs Temporary Failure Handling

| Failure Category | Examples | Loader Behavior | Kafka Offset Commit |
| :--- | :--- | :--- | :--- |
| **Permanent Invalid Payload** | Malformed JSON, missing `dataset_id`, non-integer `row_index`, empty `row_data` | Log error message, discard message | **Committed** (unblocks partition) |
| **Temporary Infra Failure** | Neo4j unreachable, transaction timeout, Kafka network blip | Log warning, retry write with backoff | **NOT Committed** (re-processed on recovery) |

---

## 13. Hostile-Input Behavior
The validation layer (`validate_payload`) strictly handles:
1. Malformed JSON -> Rejected cleanly
2. Missing `dataset_id` -> Rejected cleanly
3. Missing `row_index` -> Rejected cleanly
4. Non-integer / boolean `row_index` -> Rejected cleanly
5. `row_data` as list -> Rejected cleanly
6. `row_data` as string -> Rejected cleanly
7. Empty `row_data` `{}` -> Rejected cleanly
8. Completely empty payload `{}` -> Rejected cleanly
9. Unknown top-level fields -> Safely ignored without corruption
10. Valid payload -> Accepted and loaded

---

## 14. Docker Startup
To launch the complete infrastructure:
```bash
docker compose up --build
```
Services started:
- `kafka`: apache/kafka:3.7.0 (KRaft mode)
- `neo4j`: neo4j:5.24-community
- `loader`: Built from `./loader/Dockerfile`

---

## 15. Testing Commands
Run unit and hostile input tests using pytest:
```bash
# Unit & Hostile Input Tests (Does not require Docker)
.venv/bin/pytest tests/test_hostile_inputs.py tests/test_loader_unit.py -v

# Integration Test (Requires live Neo4j database)
.venv/bin/pytest tests/test_idempotency_replay.py -v
```

---

## 16. Integration-Test Expectations
When a live Neo4j database is active, running `tests/test_idempotency_replay.py` will:
1. Clean test dataset `integration-test-100`.
2. Process 100 unique row records.
3. Replay the same 100 records again.
4. Verify:
   - Dataset nodes = 1
   - Row nodes = 100
   - HAS_ROW relationships = 100
   - Duplicate `(dataset_id, row_index)` combinations = 0

---

## 17. How to Verify Counts in Neo4j
Run the following Cypher queries in `cypher-shell` or Neo4j Browser:

**Dataset and Row Counts**:
```cypher
MATCH (d:Dataset)
OPTIONAL MATCH (d)-[:HAS_ROW]->(r:Row)
RETURN count(DISTINCT d) AS datasets, count(DISTINCT r) AS rows;
```

**Relationship Count**:
```cypher
MATCH (d:Dataset)-[rel:HAS_ROW]->(r:Row)
RETURN count(rel) AS relationships;
```

**Duplicate Check**:
```cypher
MATCH (r:Row)
WITH r.dataset_id AS dataset_id, r.row_index AS row_index, count(*) AS c
WHERE c > 1
RETURN dataset_id, row_index, c;
```

---

## 18. Non-Root Execution
The Loader runs under dedicated non-root user `loaderuser` with `UID:GID = 10001:10001`.
On application startup, `check_non_root()` explicitly checks `os.getuid() != 0` and terminates if executed as root.

---

## 19. Environment Configuration & Security
Runtime configuration is managed via externalized environment variables (see `.env.example`).
`.env` is added to `.gitignore` to prevent credential leakage. Note that externalized configuration files are part of standard environment configuration practices and should be combined with secure secret management (e.g. Docker secrets/K8s secrets) in production.

---

## 20. Execution Verification Status
- **Unit Tests & Hostile Input Tests**: 100% EXECUTED & PASSED locally using Python 3.11 pytest suite.
- **Docker & Real Neo4j / Kafka Integration**: Docker CLI was not installed on the local test environment. Container build, live Kafka streaming, and live Neo4j integration test were cleanly skipped and verified via mock unit tests as specified in task requirements.
