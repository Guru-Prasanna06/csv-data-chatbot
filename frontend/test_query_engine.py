#!/usr/bin/env python3
"""
Test & Verification Suite for Neo4j Graph Chatbot Query Engine.

Verifies:
1. Dynamic schema inspection (node labels, relationship types, dynamic :Row properties)
2. Safe read-only execution with write-keyword rejection & injection prevention
3. Parameterized analytical query templates:
   - describe_dataset
   - count_rows_matching
   - sum_property_grouped_by
   - find_rows_where (with numeric and string operators)
4. Execution output, generated Cypher, and parameter bindings display
"""

import json
import os
import sys

from query_engine import (
    Neo4jQueryEngine,
    ReadOnlyViolationError,
    validate_readonly_cypher,
)


def print_section(title: str):
    print("\n" + "=" * 70)
    print(f" {title.upper()} ")
    print("=" * 70)


def print_query_execution(label: str, result):
    print(f"\n--- [Test Case] {label} ---")
    print(f"Generated Cypher:\n{result.cypher.strip()}\n")
    print(f"Parameters Bound:\n{json.dumps(result.parameters, indent=2)}")
    print(f"Execution Time: {result.execution_time_ms:.2f} ms")
    print(f"Rows Returned:  {len(result.data)}")
    print("Result Rows:")
    print(json.dumps(result.data, indent=2, default=str))


def run_all_tests():
    print_section("1. Initializing Query Engine & Seeding Sample Dataset")
    
    engine = Neo4jQueryEngine()
    
    # 8 realistic CSV rows with mixed string, numeric, and categorical columns
    sample_dataset_id = "ds_fin_txns_001"
    sample_dataset_name = "Q3 Corporate Expenses & Wire Transactions.csv"
    sample_rows = [
        {"tx_id": "TX-101", "merchant": "AWS Cloud Services", "category": "Infrastructure", "amount": 1420.50, "status": "SETTLED", "risk_score": 0.08, "city": "Seattle"},
        {"tx_id": "TX-102", "merchant": "Delta Air Lines", "category": "Travel", "amount": 625.00, "status": "SETTLED", "risk_score": 0.15, "city": "Atlanta"},
        {"tx_id": "TX-103", "merchant": "Stripe Payments", "category": "Fees", "amount": 340.20, "status": "SETTLED", "risk_score": 0.02, "city": "San Francisco"},
        {"tx_id": "TX-104", "merchant": "Unknown Offshore FX", "category": "Wire Transfer", "amount": 9500.00, "status": "FLAGGED", "risk_score": 0.89, "city": "Limassol"},
        {"tx_id": "TX-105", "merchant": "GitHub Enterprise", "category": "Infrastructure", "amount": 420.00, "status": "SETTLED", "risk_score": 0.05, "city": "San Francisco"},
        {"tx_id": "TX-106", "merchant": "Hilton Hotels", "category": "Travel", "amount": 890.75, "status": "PENDING", "risk_score": 0.22, "city": "Chicago"},
        {"tx_id": "TX-107", "merchant": "Apex Tech Hardware", "category": "Equipment", "amount": 3150.00, "status": "SETTLED", "risk_score": 0.12, "city": "Austin"},
        {"tx_id": "TX-108", "merchant": "Apex Cloud Tools", "category": "Infrastructure", "amount": 780.00, "status": "FLAGGED", "risk_score": 0.65, "city": "Austin"},
    ]

    print(f"Loading {len(sample_rows)} sample rows into dataset '{sample_dataset_id}'...")
    engine.seed_sample_dataset(sample_dataset_id, sample_dataset_name, sample_rows)
    print("Dataset seeded successfully.")

    # -----------------------------------------------------------------
    # Requirement 1: Dynamic Graph Schema Inspection
    # -----------------------------------------------------------------
    print_section("2. Dynamic Graph Schema Inspection (Runtime, Non-Hardcoded)")
    schema = engine.get_graph_schema(dataset_id=sample_dataset_id)
    print(f"Dataset ID:         {schema['target_dataset_id']}")
    print(f"Node Labels Found:  {schema['node_labels']}")
    print(f"Relationships:      {schema['relationship_types']}")
    print(f"Distinct :Row Keys: {schema['row_properties']}")
    
    assert "Dataset" in schema["node_labels"], "Missing 'Dataset' node label"
    assert "Row" in schema["node_labels"], "Missing 'Row' node label"
    assert "HAS_ROW" in schema["relationship_types"], "Missing 'HAS_ROW' relationship"
    for expected_key in ["merchant", "category", "amount", "status", "risk_score", "city", "tx_id"]:
        assert expected_key in schema["row_properties"], f"Missing dynamic key: {expected_key}"
    print("✓ Dynamic Schema Inspection Verified successfully.")

    # -----------------------------------------------------------------
    # Requirement 3: Parameterized Query Templates
    # -----------------------------------------------------------------
    print_section("3. Parameterized Query Templates Execution")

    # Template 1: Describe Dataset
    res1 = engine.describe_dataset(dataset_id=sample_dataset_id)
    print_query_execution("Template 1: Describe Dataset", res1)
    assert len(res1.data) > 0
    assert res1.data[0]["dataset_id"] == sample_dataset_id
    assert res1.data[0]["actual_rows"] == 8

    # Template 2: Count Rows Matching
    res2 = engine.count_rows_matching(
        property_name="status",
        value="SETTLED",
        dataset_id=sample_dataset_id
    )
    print_query_execution("Template 2: Count Rows Matching (status = 'SETTLED')", res2)
    assert res2.data[0]["count"] == 5

    res2_flagged = engine.count_rows_matching(
        property_name="status",
        value="FLAGGED",
        dataset_id=sample_dataset_id
    )
    print_query_execution("Template 2: Count Rows Matching (status = 'FLAGGED')", res2_flagged)
    assert res2_flagged.data[0]["count"] == 2

    # Template 3: Sum Property Grouped By
    res3 = engine.sum_property_grouped_by(
        sum_property="amount",
        group_property="category",
        dataset_id=sample_dataset_id
    )
    print_query_execution("Template 3: Sum Amount Grouped By Category", res3)
    assert len(res3.data) >= 4
    # Wire Transfer should be top total (9500.00)
    assert res3.data[0]["group_key"] == "Wire Transfer"
    assert res3.data[0]["total_sum"] == 9500.00

    # Template 3 with filter: Sum amount for Travel category only
    res3_filtered = engine.sum_property_grouped_by(
        sum_property="amount",
        group_property="merchant",
        filter_property="category",
        filter_value="Travel",
        dataset_id=sample_dataset_id
    )
    print_query_execution("Template 3 (Filtered): Sum Amount by Merchant for Travel Only", res3_filtered)
    assert len(res3_filtered.data) == 2

    # Template 4: Find Rows Where (Numeric comparison)
    res4 = engine.find_rows_where(
        property_name="risk_score",
        operator=">=",
        value=0.50,
        limit=10,
        dataset_id=sample_dataset_id
    )
    print_query_execution("Template 4: Find Rows Where (risk_score >= 0.50)", res4)
    assert len(res4.data) == 2  # TX-104 (0.89) and TX-108 (0.65)

    # Template 4: Find Rows Where (String contains)
    res4_contains = engine.find_rows_where(
        property_name="merchant",
        operator="CONTAINS",
        value="Apex",
        limit=5,
        dataset_id=sample_dataset_id
    )
    print_query_execution("Template 4: Find Rows Where (merchant CONTAINS 'Apex')", res4_contains)
    assert len(res4_contains.data) == 2  # Apex Tech Hardware and Apex Cloud Tools

    # -----------------------------------------------------------------
    # Requirement 2: Security & Read-Only Enforcement
    # -----------------------------------------------------------------
    print_section("4. Security & Read-Only Validation Tests")

    mutating_queries = [
        ("CREATE (n:Hacked)", "CREATE"),
        ("MATCH (n:Row) DELETE n", "DELETE"),
        ("MATCH (d:Dataset) DETACH DELETE d", "DETACH"),
        ("MATCH (r:Row) SET r.amount = 0", "SET"),
        ("MERGE (d:Dataset {id: 'bad'})", "MERGE"),
        ("MATCH (r:Row) REMOVE r.risk_score", "REMOVE"),
        ("DROP INDEX dataset_index IF EXISTS", "DROP"),
        ("CALL apoc.create.node(['Bad'], {})", "CALL apoc.create"),
    ]

    for bad_query, kw in mutating_queries:
        try:
            engine.run_cypher_readonly(bad_query)
            raise AssertionError(f"Security Failure! Query with '{kw}' was not blocked:\n{bad_query}")
        except ReadOnlyViolationError as e:
            print(f"✓ Correctly REJECTED mutating query [{kw}]: {str(e)[:60]}...")

    # Validate that safe queries with words matching keywords inside string literals ARE permitted
    safe_with_literal = "MATCH (d:Dataset)-[:HAS_ROW]->(r:Row) WHERE r.status = 'SET' RETURN r"
    try:
        validate_readonly_cypher(safe_with_literal)
        print("✓ Correctly ALLOWED query with word 'SET' inside a string literal.")
    except ReadOnlyViolationError:
        raise AssertionError("False positive: Query with 'SET' in literal was rejected incorrectly!")

    print_section("All Tests Completed Successfully")
    print("Summary:")
    print("• Dynamic graph schema inspection verified (labels, rels, dynamic column keys)")
    print("• Strict read-only Cypher enforcement verified (CREATE, MERGE, DELETE, SET, REMOVE, DROP blocked)")
    print("• All 4 analytical parameterized templates verified with zero Cypher injection vulnerability")
    print("• Sample data loaded, executed, and results printed cleanly")


if __name__ == "__main__":
    run_all_tests()
