#!/usr/bin/env python3
"""
Test Suite for Natural-Language-to-Cypher Translation Layer.

Tests at least 10 varied phrasings across the 5 categories:
1. Counting questions
2. Aggregation questions (including fuzzy synonym column matching like 'spending' -> 'amount')
3. Filter / lookup questions (parsing operators like 'above $5000', '>=', 'contains')
4. Entity-specific lookups (IDs like CUST-8912 or TX-104, named merchants)
5. Dataset overview questions ('what data do you have', 'describe this dataset')
6. Unmatched questions (ensure confidence threshold prevents forced hallucinated queries)
"""

import json
from translator import translate_question, resolve_column_name


def run_translation_tests():
    # Schema from Stage 1 sample dataset
    schema = {
        "target_dataset_id": "ds_fin_txns_001",
        "node_labels": ["Dataset", "Row"],
        "relationship_types": ["HAS_ROW"],
        "row_properties": [
            "amount",
            "category",
            "city",
            "merchant",
            "risk_score",
            "status",
            "tx_id"
        ]
    }

    test_queries = [
        # --- Category 1: Counting ---
        {
            "category": "1. Counting Questions",
            "question": "How many rows are in this dataset?",
            "expected_template": "count_rows_matching",
        },
        {
            "category": "1. Counting Questions",
            "question": "Count of rows where status is SETTLED",
            "expected_template": "count_rows_matching",
        },
        {
            "category": "1. Counting Questions",
            "question": "How many FLAGGED transactions are there?",
            "expected_template": "count_rows_matching",
        },

        # --- Category 2: Aggregation & Fuzzy Column Matching ---
        {
            "category": "2. Aggregation & Fuzzy Matching",
            "question": "Total spending by category",
            "expected_template": "sum_property_grouped_by",
        },
        {
            "category": "2. Aggregation & Fuzzy Matching",
            "question": "Sum of amount grouped by merchant",
            "expected_template": "sum_property_grouped_by",
        },
        {
            "category": "2. Aggregation & Fuzzy Matching",
            "question": "What is the total spending per city?",
            "expected_template": "sum_property_grouped_by",
        },

        # --- Category 3: Filter / Lookup with Operators ---
        {
            "category": "3. Filter / Lookup Questions",
            "question": "Show me transactions above $5000",
            "expected_template": "find_rows_where",
        },
        {
            "category": "3. Filter / Lookup Questions",
            "question": "Find rows where risk_score >= 0.5",
            "expected_template": "find_rows_where",
        },
        {
            "category": "3. Filter / Lookup Questions",
            "question": "Which rows have merchant contains Apex",
            "expected_template": "find_rows_where",
        },

        # --- Category 4: Entity-Specific Lookups ---
        {
            "category": "4. Entity-Specific Lookups",
            "question": "What is the total for customer CUST-8912",
            "expected_template": "sum_property_grouped_by",
        },
        {
            "category": "4. Entity-Specific Lookups",
            "question": "Show transactions for tx TX-101",
            "expected_template": "find_rows_where",
        },
        {
            "category": "4. Entity-Specific Lookups",
            "question": "What is the total spending for merchant Delta Air Lines",
            "expected_template": "sum_property_grouped_by",
        },

        # --- Category 5: Dataset Overview ---
        {
            "category": "5. Dataset Overview",
            "question": "What data do you have?",
            "expected_template": "describe_dataset",
        },
        {
            "category": "5. Dataset Overview",
            "question": "Describe this dataset",
            "expected_template": "describe_dataset",
        },
        {
            "category": "5. Dataset Overview",
            "question": "What can I ask about?",
            "expected_template": "describe_dataset",
        },

        # --- Negative / Unmatched Cases ---
        {
            "category": "6. Out-of-Domain / Low Confidence",
            "question": "What is the weather in Tokyo right now?",
            "expected_template": None,
        },
        {
            "category": "6. Out-of-Domain / Low Confidence",
            "question": "Write a sonnet about quantum physics",
            "expected_template": None,
        }
    ]

    print("=" * 80)
    print(" NATURAL LANGUAGE TO CYPHER TRANSLATION TEST SUITE ")
    print("=" * 80)
    print(f"Dynamic Schema Columns: {schema['row_properties']}")
    print(f"Active Dataset ID:      {schema['target_dataset_id']}\n")

    passed = 0
    total = len(test_queries)

    for idx, t in enumerate(test_queries, 1):
        q = t["question"]
        expected_template = t["expected_template"]
        res = translate_question(q, schema)

        matched = res.get("matched", False)
        template_used = res.get("template_used")

        is_success = False
        if expected_template is None:
            # Expect matched: False
            is_success = (matched is False)
        else:
            is_success = (matched is True and template_used == expected_template)

        status_str = "✓ PASS" if is_success else "✗ FAIL"
        if is_success:
            passed += 1

        print(f"[{idx:02d}/{total:02d}] {status_str} | Category: {t['category']}")
        print(f"     Question: \"{q}\"")
        if matched:
            print(f"     Template: {template_used}")
            print(f"     Params:   {json.dumps(res.get('params', {}))}")
            # Print single-line Cypher preview
            cypher_preview = " ".join(res.get('cypher', '').split())
            print(f"     Cypher:   {cypher_preview[:90]}...")
        else:
            print(f"     Matched:  False (Reason: {res.get('reason')})")
        print()

    # Verify fuzzy resolver directly on varied synonyms
    print("=" * 80)
    print(" VERIFYING FUZZY COLUMN RESOLUTION ")
    print("=" * 80)
    synonym_tests = [
        ("spending", "amount"),
        ("spend", "amount"),
        ("cost", "amount"),
        ("vendor", "merchant"),
        ("seller", "merchant"),
        ("location", "city"),
        ("score", "risk_score"),
        ("risk", "risk_score"),
        ("transaction", "tx_id"),
        ("state", "status")
    ]
    for term, expected_target in synonym_tests:
        resolved = resolve_column_name(term, schema["row_properties"])
        match_ok = (resolved == expected_target)
        print(f"   Term: '{term}' -> Resolved: '{resolved}' (Expected: '{expected_target}') [{'OK' if match_ok else 'FAIL'}]")
        assert match_ok, f"Fuzzy resolution failed for term: {term}"

    # -----------------------------------------------------------------
    # End-to-End Execution of Translated Cypher Queries
    # -----------------------------------------------------------------
    print("=" * 80)
    print(" END-TO-END EXECUTION: RUNNING TRANSLATED CYPHER QUERIES ")
    print("=" * 80)
    from query_engine import Neo4jQueryEngine
    engine = Neo4jQueryEngine()
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
    engine.seed_sample_dataset("ds_fin_txns_001", "Financial Transactions", sample_rows)

    e2e_count = 0
    for t in test_queries:
        if t["expected_template"] is not None:
            q = t["question"]
            res = translate_question(q, schema)
            assert res["matched"] is True
            # Execute through read-only validator & engine
            q_res = engine.run_cypher_readonly(res["cypher"], res["params"])
            print(f"✓ Executed \"{q}\" -> {len(q_res.data)} result row(s) in {q_res.execution_time_ms:.2f}ms")
            e2e_count += 1

    print(f"\n✓ Successfully executed all {e2e_count} translated queries against query engine.")

    print("\n" + "=" * 80)
    print(f" TEST SUMMARY: {passed}/{total} tests passed ({passed/total*100:.1f}%) | {e2e_count} end-to-end executions verified")
    print("=" * 80)

    assert passed == total, f"Expected all {total} tests to pass, but {passed} passed."


if __name__ == "__main__":
    run_translation_tests()
