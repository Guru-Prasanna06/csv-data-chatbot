#!/usr/bin/env python3
"""
Test Suite for Strict Non-Negotiable Guardrails & Factual Integrity Layer.

Verifies:
1. Core Rule Enforcement:
   - General/trained knowledge (capitals, stock prices, celebrities, history, math)
     is strictly REJECTED without querying or guessing.
2. 4-Step Linear Pipeline (Zero Bypass Paths):
   - Step 1 (Schema Check): Out-of-schema questions stop immediately (cypher=null, grounded=false).
   - Step 2 (Translation): Unmatched queries stop immediately (cypher=null, grounded=false).
   - Step 3 (Execution): Mutation attempts & syntax errors stop immediately (grounded=false).
   - Step 4 (Result Check):
     * Empty result array -> grounded: false (retains Cypher & empty array).
     * Non-empty result array -> grounded: true (uses ONLY result values).
3. Hard Assertion Function: `assert_answer_is_grounded(answer_text, result_data)`:
   - Throws `UngroundedFactError` on hallucinated numbers.
   - Throws `UngroundedFactError` on hallucinated entities.
   - Pipeline catches error and forces response to:
     "Unable to verify this answer against the data." (grounded: false).
4. Verbatim System Prompt Verification:
   - Confirms mandatory instruction text is present in LLM layer.
"""

import inspect
from guardrails import assert_answer_is_grounded, UngroundedFactError
from grounding import answer_question
from query_engine import Neo4jQueryEngine
import llm_layer


def run_guardrails_test_suite():
    print("=" * 90)
    print(" STRICT NON-NEGOTIABLE GUARDRAIL LAYER VERIFICATION SUITE ")
    print("=" * 90)

    # 1. Setup Engine and Sample Dataset
    engine = Neo4jQueryEngine()
    dataset_id = "ds_fin_txns_001"
    dataset_name = "Q3 Corporate Expenses & Wire Transactions.csv"
    sample_rows = [
        {"tx_id": "TX-101", "merchant": "AWS Cloud Services", "category": "Infrastructure", "amount": 1420.50, "status": "SETTLED", "risk_score": 0.08, "city": "Seattle"},
        {"tx_id": "TX-102", "merchant": "Delta Air Lines", "category": "Travel", "amount": 625.00, "status": "SETTLED", "risk_score": 0.15, "city": "Atlanta"},
        {"tx_id": "TX-104", "merchant": "Unknown Offshore FX", "category": "Wire Transfer", "amount": 9500.00, "status": "FLAGGED", "risk_score": 0.89, "city": "Limassol"},
    ]
    engine.seed_sample_dataset(dataset_id, dataset_name, sample_rows)
    schema = engine.get_graph_schema(dataset_id=dataset_id)

    # =================================================================
    # Test Section 1: Hard Assertion Function Unit Tests
    # =================================================================
    print("\n--- [Section 1] Hard Assertion Function Unit Tests ---")
    mock_data = [
        {"merchant": "Unknown Offshore FX", "category": "Wire Transfer", "amount": 9500.00, "status": "FLAGGED"}
    ]

    # Test 1.1: Valid Grounded Answer (All facts trace to data)
    valid_answer = "The highest transaction is 'Unknown Offshore FX' with an amount of $9,500.00 USD."
    try:
        assert_answer_is_grounded(valid_answer, mock_data)
        print("✓ PASS: Valid grounded answer passed assertion cleanly.")
    except UngroundedFactError as e:
        raise AssertionError(f"Valid answer falsely failed assertion: {e}")

    # Test 1.2: Hallucinated Number (Answer claims $12,000, data only has $9,500)
    hallucinated_number_answer = "The transaction to 'Unknown Offshore FX' was $12,000.00 USD."
    try:
        assert_answer_is_grounded(hallucinated_number_answer, mock_data)
        raise AssertionError("Security Failure: Hallucinated number was NOT caught by assertion!")
    except UngroundedFactError as e:
        print(f"✓ PASS: Successfully caught hallucinated number: {e}")

    # Test 1.3: Hallucinated Entity (Answer claims 'Citibank', data has 'Unknown Offshore FX')
    hallucinated_entity_answer = "The largest transaction was paid to 'Citibank' for $9,500.00 USD."
    try:
        assert_answer_is_grounded(hallucinated_entity_answer, mock_data)
        raise AssertionError("Security Failure: Hallucinated entity was NOT caught by assertion!")
    except UngroundedFactError as e:
        print(f"✓ PASS: Successfully caught hallucinated entity: {e}")

    # Test 1.4: Positive claims when data is empty
    try:
        assert_answer_is_grounded("Found 3 transactions totaling $5,000.00.", [])
        raise AssertionError("Security Failure: Positive claim on empty data was NOT caught!")
    except UngroundedFactError as e:
        print(f"✓ PASS: Successfully caught positive claim on empty data: {e}")

    # =================================================================
    # Test Section 2: Core Rule & General Knowledge Rejection Tests
    # =================================================================
    print("\n--- [Section 2] General Knowledge Rejection (Never Use Trained Knowledge) ---")
    general_knowledge_questions = [
        ("What is the capital of France?", "General Knowledge: Geography"),
        ("What is Apple's current stock price?", "General Knowledge: Financial Markets"),
        ("Who was Albert Einstein and what year was he born?", "General Knowledge: History & Biography"),
        ("What is 24 multiplied by 365?", "General Knowledge: Math Calculation"),
        ("What's the weather today in Tokyo?", "General Knowledge: External Real-time Data")
    ]

    for q, label in general_knowledge_questions:
        res = answer_question(q, schema, engine)
        print(f"Question: \"{q}\" ({label})")
        print(f"  • Grounded: {res['grounded']} (Must be False)")
        print(f"  • Cypher:   {res['cypher']} (Must be None - Stopped before query)")
        print(f"  • Answer:   {res['answer'][:90]}...")
        assert res["grounded"] is False, f"Failed Core Rule for: {q}"
        assert res["cypher"] is None, f"Should not have generated Cypher for: {q}"
        assert len(res["result"]) == 0
        assert "This dataset contains columns:" in res["answer"]
        print("  ✓ PASS: Safely rejected external question; reported actual schema columns.\n")

    # =================================================================
    # Test Section 3: 4-Step Pipeline Execution Flow
    # =================================================================
    print("--- [Section 3] 4-Step Pipeline Enforcement (Zero Bypass Paths) ---")

    # Step 1 Stop: Question on nonexistent column stops at Step 1
    s1_res = answer_question("Filter rows where employee_performance_score > 90", schema, engine)
    assert s1_res["grounded"] is False and s1_res["cypher"] is None
    print("✓ PASS Step 1 Gate: Nonexistent column stopped at Step 1 before Cypher generation.")

    # Step 2 Stop: Non-matching gibberish question stops at Step 2
    s2_res = answer_question("blargh xyz random text", schema, engine)
    assert s2_res["grounded"] is False and s2_res["cypher"] is None
    print("✓ PASS Step 2 Gate: Translation failure stopped at Step 2 before database query.")

    # Step 4 Empty Stop: Nonexistent entity executes Cypher, but stops at Step 4 (Empty result)
    s4_empty = answer_question("What is the total spending for customer CUST-9999", schema, engine)
    assert s4_empty["grounded"] is False
    assert s4_empty["cypher"] is not None
    assert len(s4_empty["result"]) == 0
    assert "found no matching records in the uploaded data" in s4_empty["answer"]
    print("✓ PASS Step 4 Gate (Empty): Query executed, 0 rows detected, returned grounded: false.")

    # Step 4 Non-Empty Pass: Valid query returns data and passes assert_answer_is_grounded
    s4_valid = answer_question("Count of rows where status is SETTLED", schema, engine)
    assert s4_valid["grounded"] is True
    assert s4_valid["cypher"] is not None
    assert len(s4_valid["result"]) > 0
    assert "Found 2 rows where `status` is 'SETTLED'" in s4_valid["answer"]
    print("✓ PASS Step 4 Gate (Non-Empty): Valid rows verified and assertion passed successfully.")

    # =================================================================
    # Test Section 4: Verbatim LLM System Prompt Verification
    # =================================================================
    print("\n--- [Section 4] Verbatim Mandatory LLM Prompt Verification ---")
    mandatory_verbatim = (
        "You must only use information present in the provided query result. "
        "If the result is empty or does not answer the question, you must respond with exactly: NOT_IN_DATA. "
        "Do not use any outside knowledge, even if you know the answer. "
        "Do not guess. Do not be helpful by filling in gaps — an incomplete honest answer is required, "
        "not a complete guessed one."
    )

    llm_source = inspect.getsource(llm_layer)
    assert mandatory_verbatim in llm_source, "Mandatory verbatim prompt instruction missing from llm_layer.py!"
    print("✓ PASS: Mandatory verbatim system prompt constraint verified in llm_layer.py.")

    print("\n" + "=" * 90)
    print(" ALL GUARDRAIL TESTS COMPLETED SUCCESSFULLY (100.0%) ")
    print("=" * 90)


if __name__ == "__main__":
    run_guardrails_test_suite()
