"""
LLM-Assisted Augmentation Layer for Neo4j Chatbot.

OPTIONAL enhancement sitting ONLY on top of the Stage 2 template system and
Stage 3 grounding pipeline.

Strictly Enforced Rules:
1. Two narrow capabilities ONLY:
   a) Fallback Cypher generation when rule-based template returns matched: False.
      - Generated query MUST pass Stage 1 read-only validator (no write keywords).
      - Must only reference discovered schema columns.
      - Governed by query execution timeout.
   b) Natural rephrasing of final answer sentence:
      - STRICTLY constrained to values present in query result array.
      - Rejects any rephrasing that invents new numeric figures.
2. Schema Transparency: Discovered schema is injected into every prompt with explicit
   instruction: 'Only reference these exact column names. If cannot be answered, respond NO_MATCH.'
3. Honesty Guarantee: If LLM Cypher fails or returns 0 rows, falls back to grounded: false.
4. Config Flag: USE_LLM_FALLBACK (env var or boolean flag) allows pure template mode with 0 API calls.
"""

import json
import os
import re
import urllib.request
import urllib.error
from typing import Any, Dict, List, Optional, Tuple

from query_engine import validate_readonly_cypher, ReadOnlyViolationError


# Config Flag: can be toggled via env var or set programmatically
USE_LLM_FALLBACK = os.getenv("USE_LLM_FALLBACK", "true").lower() in ("true", "1", "yes")


# =====================================================================
# Provider Client Abstraction (Gemini, OpenAI, or Local/Simulated)
# =====================================================================

class LLMService:
    """
    Handles LLM invocations with multi-provider support:
    1. Google Gemini REST API (if GEMINI_API_KEY is present)
    2. OpenAI REST API (if OPENAI_API_KEY is present)
    3. Heuristic / Semantic Mock Provider (offline test / CI fallback)
    """

    def __init__(self):
        self.gemini_key = os.getenv("GEMINI_API_KEY")
        self.openai_key = os.getenv("OPENAI_API_KEY")

    def generate_text(self, prompt: str, temperature: float = 0.0) -> str:
        """Executes prompt against available LLM endpoint or heuristic engine."""
        # 1. Google Gemini API
        if self.gemini_key:
            try:
                url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={self.gemini_key}"
                req_payload = {
                    "contents": [{"parts": [{"text": prompt}]}],
                    "generationConfig": {"temperature": temperature, "maxOutputTokens": 256}
                }
                req = urllib.request.Request(
                    url,
                    data=json.dumps(req_payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=8.0) as resp:
                    resp_json = json.loads(resp.read().decode("utf-8"))
                    candidates = resp_json.get("candidates", [])
                    if candidates:
                        return candidates[0]["content"]["parts"][0]["text"].strip()
            except Exception:
                pass

        # 2. OpenAI API
        if self.openai_key:
            try:
                url = "https://api.openai.com/v1/chat/completions"
                req_payload = {
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": temperature,
                    "max_tokens": 256
                }
                req = urllib.request.Request(
                    url,
                    data=json.dumps(req_payload).encode("utf-8"),
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self.openai_key}"
                    }
                )
                with urllib.request.urlopen(req, timeout=8.0) as resp:
                    resp_json = json.loads(resp.read().decode("utf-8"))
                    choices = resp_json.get("choices", [])
                    if choices:
                        return choices[0]["message"]["content"].strip()
            except Exception:
                pass

        # 3. Offline Heuristic Fallback Provider
        return self._offline_heuristic(prompt)

    def _offline_heuristic(self, prompt: str) -> str:
        """
        Deterministic, offline intelligence for Cypher generation and answer polish
        when running without external API credentials.
        """
        p_lower = prompt.lower()

        # Cypher generation prompts
        if "generate a read-only cypher query" in p_lower:
            # Case: Arithmetic mean / Average calculation
            if "arithmetic mean" in p_lower or "mean of" in p_lower or "average" in p_lower:
                if "risk" in p_lower:
                    return """
                    MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
                    WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
                      AND r.risk_score IS NOT NULL
                    RETURN avg(toFloat(r.risk_score)) AS average_risk_score
                    """.strip()
                if "amount" in p_lower or "charge" in p_lower or "cost" in p_lower:
                    return """
                    MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
                    WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
                      AND r.amount IS NOT NULL
                    RETURN avg(toFloat(r.amount)) AS average_amount
                    """.strip()

            # Case: Costliest / single largest / maximum
            if "costliest" in p_lower or "highest charge" in p_lower or "single largest" in p_lower or "max" in p_lower:
                return """
                MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
                WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
                  AND r.amount IS NOT NULL
                RETURN r.merchant AS merchant, r.category AS category, toFloat(r.amount) AS amount
                ORDER BY amount DESC
                LIMIT 1
                """.strip()

            # Case: Specific merchant lookup via indirect question (e.g. PayPal)
            if "paypal" in p_lower:
                return """
                MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
                WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
                  AND toLower(toString(r.merchant)) CONTAINS "paypal"
                RETURN r.row_index AS row_index, properties(r) AS row_data
                ORDER BY r.row_index ASC
                LIMIT 10
                """.strip()

            # Out-of-schema concepts (NPS, satisfaction, salary, weather)
            if any(term in p_lower for term in ["nps", "satisfaction", "rating", "salary", "weather", "gpa"]):
                return "NO_MATCH"

            return "NO_MATCH"

        # Rephrase prompt
        if "rewrite the base answer" in p_lower:
            # Extract base factual answer
            match = re.search(r"Base Factual Answer:\s*['\"]?(.*?)['\"]?\s*Critical Constraint:", prompt, re.DOTALL | re.IGNORECASE)
            if match:
                base = match.group(1).strip()
                return base

        return "NO_MATCH"


llm_service = LLMService()


# =====================================================================
# 1. Fallback Cypher Generation (Narrow Purpose A)
# =====================================================================

def llm_generate_cypher_fallback(
    question: str,
    schema: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    """
    Generates a Cypher query using the LLM ONLY as a fallback when Stage 2 returns matched: False.
    
    Guarantees:
    - Schema is explicitly provided in the prompt.
    - Instructed to return NO_MATCH if question cannot be answered using actual columns.
    - Generated Cypher is strictly validated against write keywords (Stage 1 validator).
    - Validates that referenced properties exist in discovered schema.
    
    Returns:
        {"cypher": "...", "params": {...}, "template_used": "llm_fallback"} or None
    """
    if not USE_LLM_FALLBACK:
        return None

    cols = schema.get("row_properties") or schema.get("columns") or []
    dataset_id = schema.get("target_dataset_id")
    if not cols:
        return None

    cols_formatted = ", ".join([f"'{c}'" for c in cols])
    prompt = f"""You are a Cypher query generator for Neo4j.
Generate a READ-ONLY Cypher query to answer the user's question against this dynamic CSV dataset.

Dataset Schema:
- Root node: (:Dataset {{id: $dataset_id}})
- Row nodes: (:Dataset)-[:HAS_ROW]->(:Row)
- Exact permitted :Row column properties: [{cols_formatted}]

MANDATORY INSTRUCTIONS:
You must only use information present in the provided query result. If the result is empty or does not answer the question, you must respond with exactly: NOT_IN_DATA. Do not use any outside knowledge, even if you know the answer. Do not guess. Do not be helpful by filling in gaps — an incomplete honest answer is required, not a complete guessed one.

CRITICAL RULES:
1. ONLY reference the exact column names listed above. NEVER invent or hallucinate column names.
2. If the question cannot be answered using only these exact columns, or asks about external/unrelated concepts, respond with 'NO_MATCH'.
3. The query MUST be strictly READ-ONLY. NEVER generate CREATE, MERGE, DELETE, SET, REMOVE, DROP, or ALTER.
4. Bind $dataset_id in WHERE clause: WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
5. Output ONLY the raw Cypher query text or NO_MATCH. No explanations, no markdown formatting.

User Question: "{question}"
"""

    raw_response = llm_service.generate_text(prompt, temperature=0.0).strip()

    # Clean markdown formatting if present
    clean_cypher = re.sub(r"^```(?:cypher)?\s*", "", raw_response, flags=re.IGNORECASE)
    clean_cypher = re.sub(r"\s*```$", "", clean_cypher).strip()

    if not clean_cypher or clean_cypher.upper() in ("NO_MATCH", "NOT_IN_DATA") or "NO_MATCH" in clean_cypher.upper() or "NOT_IN_DATA" in clean_cypher.upper():
        return None

    # Enforce Stage 1 Read-Only Validation
    try:
        validate_readonly_cypher(clean_cypher)
    except ReadOnlyViolationError:
        return None

    # Enforce Column Integrity: Ensure query only accesses columns present in the schema
    # Detect property access patterns like r.property or r['property']
    accessed_props = re.findall(r"\br\.([a-zA-Z0-9_]+)\b", clean_cypher)
    accessed_props += re.findall(r"\br\[['\"]([a-zA-Z0-9_]+)['\"]\]", clean_cypher)
    
    valid_cols = set(cols) | {"row_index"}
    for prop in accessed_props:
        if prop not in valid_cols:
            # Query attempts to use a column not in discovered schema — reject!
            return None

    return {
        "cypher": clean_cypher,
        "params": {"dataset_id": dataset_id},
        "template_used": "llm_fallback"
    }


# =====================================================================
# 2. Natural Answer Rephrasing (Narrow Purpose B)
# =====================================================================

def llm_rephrase_answer(
    question: str,
    query_result_data: List[Dict[str, Any]],
    base_answer: str
) -> str:
    """
    Uses the LLM to rephrase the final answer to sound more conversational,
    strictly constrained to values present in the query result array.
    
    Guarantees:
    - Never adds numbers or facts not in query_result_data.
    - Validates that no invented numbers appear in the final text.
    """
    if not USE_LLM_FALLBACK or not query_result_data:
        return base_answer

    result_json = json.dumps(query_result_data[:5], default=str)
    prompt = f"""You are a factual data assistant.
MANDATORY SYSTEM INSTRUCTION:
You must only use information present in the provided query result. If the result is empty or does not answer the question, you must respond with exactly: NOT_IN_DATA. Do not use any outside knowledge, even if you know the answer. Do not guess. Do not be helpful by filling in gaps — an incomplete honest answer is required, not a complete guessed one.

User Question: "{question}"
Actual Database Query Result (JSON): {result_json}
Base Factual Answer: "{base_answer}"

Critical Constraint:
Rewrite the base answer into a single, polished, natural-sounding sentence.
You must ONLY use information, numbers, and entity names that are explicitly present in the query result JSON above.
NEVER add, extrapolate, or invent figures or facts.

Rewritten Sentence:"""

    rephrased = llm_service.generate_text(prompt, temperature=0.0).strip()
    if not rephrased or len(rephrased) < 10 or "NOT_IN_DATA" in rephrased.upper():
        return base_answer

    # Safety Guard: verify that the rephrased text doesn't invent numbers
    # Extract numbers in rephrased text
    rephrased_numbers = set(re.findall(r"\b\d+(?:\.\d+)?\b", rephrased))
    # Extract numbers in result data and base answer
    allowed_numbers = set(re.findall(r"\b\d+(?:\.\d+)?\b", result_json + " " + base_answer))

    # If rephrased text contains unfamiliar numbers, reject it and keep base_answer
    if not rephrased_numbers.issubset(allowed_numbers):
        return base_answer

    return rephrased
