"""
Centralized Cypher Queries for Neo4j Loader.
Mandatory Graph Model: (:Dataset)-[:HAS_ROW]->(:Row)
"""

# Mandatory Idempotent Write Query
MERGE_DATASET_AND_ROW_QUERY = """
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
"""

# Database verification query
DB_VERIFY_QUERY = """
RETURN 1 AS result
"""

# Verification Queries for Integration & Testing
VERIFY_DATASET_ROW_COUNTS_QUERY = """
MATCH (d:Dataset)
OPTIONAL MATCH (d)-[:HAS_ROW]->(r:Row)
RETURN count(DISTINCT d) AS datasets,
       count(DISTINCT r) AS rows
"""

VERIFY_RELATIONSHIP_COUNT_QUERY = """
MATCH (d:Dataset)-[rel:HAS_ROW]->(r:Row)
RETURN count(rel) AS relationships
"""

VERIFY_DUPLICATES_QUERY = """
MATCH (r:Row)
WITH r.dataset_id AS dataset_id,
     r.row_index AS row_index,
     count(*) AS c
WHERE c > 1
RETURN dataset_id, row_index, c
"""

CLEAN_TEST_DATASET_QUERY = """
MATCH (d:Dataset {id: $dataset_id})
OPTIONAL MATCH (d)-[:HAS_ROW]->(r:Row)
DETACH DELETE d, r
"""
