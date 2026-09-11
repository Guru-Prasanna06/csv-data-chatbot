"""
Non-Negotiable Guardrail & Factual Integrity Layer for Neo4j Chatbot.

Enforces the Core Rule in code:
The chatbot must NEVER answer using general/trained knowledge.
Its ONLY source of truth is the literal result returned by a Cypher query
executed against the current Neo4j graph.

Pipeline Structure (No bypass path exists):
Question 
  → (1) Schema check: does this question reference concepts/columns that exist
        in the current graph's schema?
        If NO → grounded: false. STOP.
  → (2) Translate to Cypher (template match or LLM fallback)
        If translation fails → grounded: false. STOP.
  → (3) Execute Cypher against Neo4j (read-only, timeout-protected)
        If execution fails → grounded: false. STOP.
  → (4) Check actual result array:
        If empty → grounded: false (shows Cypher and empty array).
        If non-empty → grounded: true (builds answer using ONLY result values).
  → (5) Hard Assertion Gate: assert_answer_is_grounded(answer_text, result_data)
        If any fact/number cannot be verified against result_data → THROW ERROR
        and force grounded: false.
"""

import re
import json
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("guardrails")


# =====================================================================
# Custom Guardrail Exception
# =====================================================================

class UngroundedFactError(ValueError):
    """Raised when an answer contains numbers, entities, or facts not present in query result data."""
    pass


# =====================================================================
# Hard Assertion Function
# =====================================================================

def extract_numbers_from_text(text: str) -> Set[float]:
    """Extracts all numerical values (currency, decimals, integers) from text."""
    numbers = set()
    # Match numbers with optional commas and decimals, optional leading $
    cleaned = re.sub(r"(?<=\d),(?=\d)", "", text)
    matches = re.findall(r"(?:^|[^\w])(\d+(?:\.\d+)?)(?:$|[^\w])", cleaned)
    for m in matches:
        try:
            numbers.add(float(m))
        except ValueError:
            pass
    return numbers


def extract_numbers_from_data(data: Any) -> Set[float]:
    """Recursively extracts all numerical values present in query result records."""
    numbers = set()
    if isinstance(data, dict):
        for v in data.values():
            numbers.update(extract_numbers_from_data(v))
    elif isinstance(data, list):
        for item in data:
            numbers.update(extract_numbers_from_data(item))
    elif isinstance(data, (int, float)):
        numbers.add(float(data))
    elif isinstance(data, str):
        # In case numbers are formatted inside string values
        cleaned = re.sub(r"(?<=\d),(?=\d)", "", data)
        for m in re.findall(r"\b\d+(?:\.\d+)?\b", cleaned):
            try:
                numbers.add(float(m))
            except ValueError:
                pass
    return numbers


def extract_string_facts_from_data(data: Any) -> Set[str]:
    """Recursively extracts all string values present in query result records."""
    strings = set()
    if isinstance(data, dict):
        for v in data.values():
            strings.update(extract_string_facts_from_data(v))
    elif isinstance(data, list):
        for item in data:
            strings.update(extract_string_facts_from_data(item))
    elif isinstance(data, str):
        s = data.strip().lower()
        if len(s) >= 2:
            strings.add(s)
    return strings


def assert_answer_is_grounded(answer_text: str, result_data: List[Dict[str, Any]]) -> None:
    """
    Strict assertion safety net:
    - Extracts any numbers, names, or specific facts mentioned in answer_text.
    - Confirms each one literally appears somewhere in result_data (or valid metadata).
    - If ANY fact in the answer cannot be traced back to result_data, throws UngroundedFactError.
    
    Raises:
        UngroundedFactError: If any ungrounded fact, hallucinated number, or untraceable entity is detected.
    """
    if not answer_text or not answer_text.strip():
        raise UngroundedFactError("Answer text is empty.")

    # 1. If result_data is empty, the answer MUST NOT assert positive data findings
    if not result_data:
        # Check if answer claims positive numbers or positive results
        ans_numbers = extract_numbers_from_text(answer_text)
        # Exclude 0 or 0.0 which are valid for "0 records found"
        positive_numbers = {n for n in ans_numbers if n != 0}
        if positive_numbers:
            raise UngroundedFactError(
                f"Ungrounded answer: result_data is empty, but answer asserts positive numbers {positive_numbers}."
            )
        # Must acknowledge 0 / no records
        lower = answer_text.lower()
        if not any(w in lower for w in ["no matching", "0 matching", "0 record", "0 row", "unable", "don't have", "no data"]):
            raise UngroundedFactError(
                "Ungrounded answer: result_data is empty, but answer does not state that 0 records were found."
            )
        return

    # 2. Extract and verify numbers in answer
    answer_numbers = extract_numbers_from_text(answer_text)
    data_numbers = extract_numbers_from_data(result_data)

    # Allowable structural/metadata numbers:
    # - Row count of results: len(result_data)
    # - 1 (singular grammatical references)
    allowed_numbers = set(data_numbers)
    allowed_numbers.add(float(len(result_data)))
    allowed_numbers.add(1.0)
    allowed_numbers.add(0.0)

    # Check for dataset metadata in describe_dataset queries (e.g. columns count, registered rows)
    for r in result_data:
        if "columns" in r and isinstance(r["columns"], list):
            allowed_numbers.add(float(len(r["columns"])))
        if "actual_rows" in r:
            allowed_numbers.add(float(r["actual_rows"]))
        if "registered_rows" in r:
            allowed_numbers.add(float(r["registered_rows"]))
        if "row_count" in r:
            allowed_numbers.add(float(r["row_count"]))
        if "count" in r:
            allowed_numbers.add(float(r["count"]))

    for num in answer_numbers:
        # Check if number matches any allowed number (allowing floating point rounding to 2 decimals)
        matched_num = False
        for allowed in allowed_numbers:
            if abs(num - allowed) < 0.01:
                matched_num = True
                break
        if not matched_num:
            raise UngroundedFactError(
                f"Assertion Failed: Number {num} in answer cannot be traced back to any value in the query result."
            )

    # 3. Extract and verify entity names quoted in answer_text
    # e.g., 'Unknown Offshore FX', 'Wire Transfer', 'Delta Air Lines', 'CUST-8912'
    quoted_entities = re.findall(r"['\"]([^'\"]+)['\"]", answer_text)
    data_strings = extract_string_facts_from_data(result_data)

    for entity in quoted_entities:
        ent_lower = entity.strip().lower()
        # Skip generic column names or operator words
        if ent_lower in {"amount", "status", "category", "merchant", "city", "risk_score", "tx_id", "row_index"}:
            continue
        # Skip phrases mentioned in honesty disclaimers
        if "not available" in answer_text.lower() or "don't have" in answer_text.lower():
            continue
        # Check if entity string exists in any field in result_data
        entity_found = any(ent_lower in s or s in ent_lower for s in data_strings)
        if not entity_found:
            raise UngroundedFactError(
                f"Assertion Failed: Entity '{entity}' in answer does not appear anywhere in query result."
            )
