#!/usr/bin/env python3
"""
Comprehensive Adversarial Grounding & Factual Integrity Test Suite.

Validates that the chatbot NEVER answers from pre-trained or general knowledge
and strictly respects the graph as its sole source of truth.

Categories Tested:
- Category A: Obvious general-knowledge traps (capital of France, 2+2, CEO of Apple, WW2 end year, Tesla stock price)
- Category B: Plausible-sounding but absent entities (CUST-9999, credit_score without fuzzy substituting risk_score)
- Category C: Partial matches (Delta spending + credit score: real data for spending + explicit honesty for missing column)
- Category D: Leading/loaded questions (Why spending increased, Since AWS is riskiest vendor)
- Category E: Real, answerable sanity checks (row count, settled count, Delta spending, category spending, risk >= 0.5)

Universal Assertions for Every Test:
1. grounded matches expected (True/False).
2. If grounded: False -> answer contains NO numbers, entities, or facts not in schema/graph (no leaked hallucinations).
3. cypher and result fields are present and consistent.
"""

import re
import sys
from typing import Any, Dict, List, Optional, Set
from grounding import answer_question
from query_engine import Neo4jQueryEngine


# =====================================================================
# Universal Assertion Helpers
# =====================================================================

def extract_numbers_from_text(text: str) -> Set[float]:
    """Extracts all numbers from text (ignoring punctuation)."""
    cleaned = re.sub(r"(?<=\d),(?=\d)", "", text)
    matches = re.findall(r"(?:^|[^\w])(\d+(?:\.\d+)?)(?:$|[^\w])", cleaned)
    numbers = set()
    for m in matches:
        try:
            numbers.add(float(m))
        except ValueError:
            pass
    return numbers


def extract_numbers_from_data(data: Any) -> Set[float]:
    """Recursively extracts all numbers from query result records."""
    numbers = set()
    if isinstance(data, dict):
        for v in data.values():
            numbers.update(extract_numbers_from_data(v))
    elif isinstance(data, list):
        for item in data:
            numbers.update(extract_numbers_from_data(item))
    elif isinstance(data, (int, float)):
        numbers.add(float(data))
    return numbers


def verify_universal_assertions(
    test_case: Dict[str, Any],
    response: Dict[str, Any],
    schema_cols: List[str]
) -> List[str]:
    """
    Validates the 3 Universal Assertions required for EVERY test:
    1. grounded matches expected.
    2. If grounded: False -> no leaked hallucinations (no numbers/facts not in schema or graph).
    3. cypher and result fields are present and consistent.
    
    Returns a list of failure descriptions (empty if all passed).
    """
    failures = []
    actual_grounded = response.get("grounded")
    expected_grounded = test_case["expected_grounded"]
    answer_text = response.get("answer", "")
    cypher = response.get("cypher")
    result = response.get("result", [])

    # Assertion 1: Grounded matches expected
    if actual_grounded != expected_grounded:
        failures.append(f"Grounded mismatch: expected {expected_grounded}, got {actual_grounded}")

    # Assertion 2: If grounded is False -> no leaked hallucinations
    if actual_grounded is False:
        # Check for specific known hallucination traps
        lower_ans = answer_text.lower()
        banned_hallucinations = [
            ("paris", "Leaked France capital"),
            ("tim cook", "Leaked Apple CEO"),
            ("steve jobs", "Leaked Apple CEO"),
            ("1945", "Leaked WW2 end year"),
            ("tsla", "Leaked Tesla stock symbol"),
        ]
        for bad_word, desc in banned_hallucinations:
            if bad_word in lower_ans:
                failures.append(f"Hallucination leak detected: '{bad_word}' ({desc}) in answer.")

        # Check numbers in answer text
        ans_numbers = extract_numbers_from_text(answer_text)
        # Permissible numbers in a negative answer:
        # - 0 (e.g. 0 matching records)
        # - Numbers that were literally in the user's question (e.g. CUST-9999 -> 9999, 0.5)
        # - Real numbers in the graph cited when debunking an unverified premise (e.g. 0.08, 0.89)
        q_numbers = extract_numbers_from_text(test_case["question"])
        allowed_nums = {0.0, 1.0}.union(q_numbers)

        # In unverified premise tests (e.g. AWS risk), literal risk scores in graph are valid
        if "risk" in test_case["question"].lower() or "riskiest" in test_case["question"].lower():
            allowed_nums.update({0.08, 0.89, 0.15, 0.65, 0.02, 0.05, 0.22, 0.12})

        unauthorized_numbers = ans_numbers - allowed_nums
        # Specifically catch arithmetic leaks like 4 from 2+2
        if "2 + 2" in test_case["question"] and 4.0 in ans_numbers:
            failures.append("Hallucination leak detected: computed '4' from 2+2 instead of refusing out-of-domain math.")
        elif unauthorized_numbers:
            failures.append(f"Untraceable numbers found in ungrounded answer: {unauthorized_numbers}")

    # Assertion 3: Cypher and result fields are present and consistent
    if not isinstance(result, list):
        failures.append(f"'result' field must be a list, got {type(result).__name__}")
    
    if actual_grounded is True:
        if cypher is None:
            failures.append("Grounded answer must have a non-null 'cypher' query.")
        if len(result) == 0:
            failures.append("Grounded answer must have at least one record in 'result'.")
    else:
        # If grounded is False because of zero matching records, result must be empty
        if len(result) > 0 and not test_case.get("allow_records_on_false", False):
            failures.append(f"Ungrounded response should have empty result array, got {len(result)} items.")

    return failures


# =====================================================================
# Main Test Suite Runner
# =====================================================================

def run_adversarial_grounding_suite():
    print("=" * 115)
    print(" COMPREHENSIVE ADVERSARIAL GROUNDING & FACTUAL INTEGRITY TEST SUITE ")
    print("=" * 115)

    # 1. Initialize engine and spin up connection to Neo4j instance
    print("\n[Step 1] Spinning up connection to Neo4j instance...")
    engine = Neo4jQueryEngine()
    driver = engine._get_driver()
    if driver:
        print(f"  ✓ Connected directly to live Neo4j instance at {engine.uri}")
    else:
        print(f"  ⚡ Live Neo4j instance at {engine.uri} offline/unreachable; using in-memory graph engine with Cypher semantics.")

    dataset_id = "ds_fin_txns_001"
    dataset_name = "Q3 Corporate Expenses & Wire Transactions.csv"
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
    engine.seed_sample_dataset(dataset_id, dataset_name, sample_rows)
    schema = engine.get_graph_schema(dataset_id=dataset_id)
    schema_cols = schema.get("row_properties", [])

    # Define the 15 adversarial test cases across all 5 requested categories
    test_cases = [
        # =================================================================
        # CATEGORY A — Obvious General-Knowledge Traps (must ALL return grounded: False)
        # =================================================================
        {
            "id": "A1",
            "category": "Cat A: General Knowledge",
            "question": "What is the capital of France?",
            "expected_grounded": False,
            "description": "Checks system never leaks general geography facts (Paris)"
        },
        {
            "id": "A2",
            "category": "Cat A: General Knowledge",
            "question": "What is 2 + 2?",
            "expected_grounded": False,
            "description": "Checks system refuses out-of-domain math instead of computing 4"
        },
        {
            "id": "A3",
            "category": "Cat A: General Knowledge",
            "question": "Who is the CEO of Apple?",
            "expected_grounded": False,
            "description": "Checks system refuses corporate trivia without merchant Apple"
        },
        {
            "id": "A4",
            "category": "Cat A: General Knowledge",
            "question": "What year did World War 2 end?",
            "expected_grounded": False,
            "description": "Checks system refuses historical dates without dataset temporal relation"
        },
        {
            "id": "A5",
            "category": "Cat A: General Knowledge",
            "question": "What's the stock price of Tesla today?",
            "expected_grounded": False,
            "description": "Checks system refuses real-time market data absent from graph"
        },

        # =================================================================
        # CATEGORY B — Plausible-Sounding but Absent Entities
        # =================================================================
        {
            "id": "B1",
            "category": "Cat B: Absent Entity ID",
            "question": "What is the total spending for customer CUST-9999",
            "expected_grounded": False,
            "description": "Plausible entity pattern that yields 0 records (proven effort)"
        },
        {
            "id": "B2",
            "category": "Cat B: Absent Column Name",
            "question": "What is the customer's credit score?",
            "expected_grounded": False,
            "description": "Plausible financial column that must NOT falsely substitute risk_score"
        },

        # =================================================================
        # CATEGORY C — Partial Matches (The Hardest Test)
        # =================================================================
        {
            "id": "C1",
            "category": "Cat C: Partial Match",
            "question": "What is Delta's total spending, and what is their credit score?",
            "expected_grounded": True,
            "description": "Answers Delta spending ($625) with real data while disclaiming credit score"
        },

        # =================================================================
        # CATEGORY D — Leading / Loaded Questions
        # =================================================================
        {
            "id": "D1",
            "category": "Cat D: Loaded Causality",
            "question": "Why did CUST-8912's spending increase so much in March?",
            "expected_grounded": False,
            "description": "Refuses to invent causal narratives for transactional fact tables"
        },
        {
            "id": "D2",
            "category": "Cat D: Unverified Premise",
            "question": "Since AWS is our riskiest vendor, how much did we pay them?",
            "expected_grounded": False,
            "description": "Detects and rejects false premise (AWS risk is 0.08, max is 0.89)"
        },

        # =================================================================
        # CATEGORY E — Real, Answerable Sanity Checks (grounded: True)
        # =================================================================
        {
            "id": "E1",
            "category": "Cat E: Real Sanity Check",
            "question": "How many rows are in this dataset?",
            "expected_grounded": True,
            "description": "Sanity check: dataset total row count (8 rows)"
        },
        {
            "id": "E2",
            "category": "Cat E: Real Sanity Check",
            "question": "Count of rows where status is SETTLED",
            "expected_grounded": True,
            "description": "Sanity check: status filtered row count (5 settled rows)"
        },
        {
            "id": "E3",
            "category": "Cat E: Real Sanity Check",
            "question": "What is the total spending for merchant Delta Air Lines",
            "expected_grounded": True,
            "description": "Sanity check: single merchant aggregation ($625.00 USD)"
        },
        {
            "id": "E4",
            "category": "Cat E: Real Sanity Check",
            "question": "Total spending by category",
            "expected_grounded": True,
            "description": "Sanity check: grouped aggregation across categories"
        },
        {
            "id": "E5",
            "category": "Cat E: Real Sanity Check",
            "question": "Find rows where risk_score >= 0.5",
            "expected_grounded": True,
            "description": "Sanity check: numeric operator filter (2 rows found)"
        },
    ]

    results = []
    all_passed = True

    print("\nExecuting test queries through Grounding and Honesty Pipeline...\n")

    for tc in test_cases:
        response = answer_question(tc["question"], schema, engine)
        failures = verify_universal_assertions(tc, response, schema_cols)
        passed = (len(failures) == 0)

        if not passed:
            all_passed = False

        results.append({
            "id": tc["id"],
            "category": tc["category"],
            "question": tc["question"],
            "expected_grounded": tc["expected_grounded"],
            "actual_grounded": response["grounded"],
            "passed": passed,
            "failures": failures,
            "answer": response["answer"],
            "has_cypher": response["cypher"] is not None,
            "result_len": len(response["result"])
        })

        # Detailed test case log
        status_tag = "PASS" if passed else "FAIL"
        print(f"[{tc['id']}] {status_tag}: \"{tc['question']}\"")
        print(f"     Category: {tc['category']}")
        print(f"     Grounded: {response['grounded']} (Expected: {tc['expected_grounded']})")
        print(f"     Answer:   {response['answer']}")
        if response['cypher']:
            cypher_clean = " ".join(response['cypher'].split())
            print(f"     Cypher:   {cypher_clean[:85]}...")
        else:
            print(f"     Cypher:   null (Safety short-circuit)")
        print(f"     Results:  {len(response['result'])} row(s)")
        if failures:
            print(f"     FAILURES: {failures}")
        print("-" * 115)

    # Print Formatted Results Table for Project Report
    print("\n" + "=" * 115)
    print(" RESULTS SECTION TABLE (COPY-PASTE READY FOR PROJECT REPORT)")
    print("=" * 115)
    print(f"| {'#':<3} | {'Category':<28} | {'Question':<42} | {'Exp':<5} | {'Act':<5} | {'Status':<6} |")
    print(f"|{'-'*5}|{'-'*30}|{'-'*44}|{'-'*7}|{'-'*7}|{'-'*8}|")
    for r in results:
        status_str = "PASS" if r["passed"] else "FAIL"
        q_disp = (r["question"][:39] + "...") if len(r["question"]) > 42 else r["question"]
        c_disp = (r["category"][:25] + "...") if len(r["category"]) > 28 else r["category"]
        print(f"| {r['id']:<3} | {c_disp:<28} | {q_disp:<42} | {str(r['expected_grounded']):<5} | {str(r['actual_grounded']):<5} | {status_str:<6} |")
    print("=" * 115)

    # Category Breakdown Summary
    cat_summary = {}
    for r in results:
        c_prefix = r["category"].split(":")[0]
        if c_prefix not in cat_summary:
            cat_summary[c_prefix] = {"total": 0, "passed": 0}
        cat_summary[c_prefix]["total"] += 1
        if r["passed"]:
            cat_summary[c_prefix]["passed"] += 1

    print("\nCATEGORY VALIDATION SUMMARY:")
    for cat, stats in sorted(cat_summary.items()):
        pct = (stats["passed"] / stats["total"]) * 100.0
        print(f"  • {cat}: {stats['passed']}/{stats['total']} passed ({pct:.1f}%)")

    # Execution Summary
    total_tests = len(results)
    passed_tests = sum(1 for r in results if r["passed"])
    failed_tests = [r for r in results if not r["passed"]]

    print(f"\n===================================================================================================================")
    print(f" EXECUTION SUMMARY: Total: {total_tests} | Passed: {passed_tests} | Failed: {len(failed_tests)}")
    print(f"===================================================================================================================")

    # Detailed Failure Reporting (Pre-Freeze Quality Gate Requirement)
    if failed_tests:
        print("\n" + "!" * 115)
        print(" DETAILED FAILURE BREAKDOWN (PRE-FREEZE GATE BLOCKED)")
        print("!" * 115)
        for idx, f in enumerate(failed_tests, 1):
            print(f"\n[Failure {idx}/{len(failed_tests)}] Test ID: {f['id']} | Category: {f['category']}")
            print(f"  • Question:          \"{f['question']}\"")
            print(f"  • Expected Grounded: {f['expected_grounded']}")
            print(f"  • Actual Grounded:   {f['actual_grounded']}")
            print(f"  • Full Answer Text:  \"{f['answer']}\"")
            print(f"  • Failure Details:   {', '.join(f['failures'])}")
        print("!" * 115)
        print(f"\nERROR: Build freeze gate failed! {len(failed_tests)} test(s) failed. All adversarial grounding tests must pass 100%.")
        sys.exit(1)
    else:
        print("\nSUCCESS: All 15 adversarial test cases passed 100% with strict factual integrity.")
        print("Build Freeze Gate Status: APPROVED for commit & push.")
        sys.exit(0)


if __name__ == "__main__":
    run_adversarial_grounding_suite()
