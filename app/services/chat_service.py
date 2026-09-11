import logging
import re
from typing import Dict, Any, List, Optional, Tuple
from app.core.config import get_settings
from app.services.neo4j_service import neo4j_service

logger = logging.getLogger(__name__)


class UnsafeCypherError(Exception):
    """Raised when a Cypher query contains dangerous or write operations."""
    pass


def validate_read_only_cypher(cypher: str) -> str:
    """
    Validates that a Cypher query is strictly read-only and safe.
    Rejects any destructive, modifying, or dangerous operations.
    """
    if not cypher or not cypher.strip():
        raise UnsafeCypherError("Cypher query cannot be empty.")

    cleaned = cypher.strip()

    # Reject semicolon query chaining
    if ";" in cleaned:
        raise UnsafeCypherError("Multi-statement Cypher queries are prohibited.")

    # Forbidden mutation and administrative keywords
    forbidden_patterns = [
        r"\bDELETE\b",
        r"\bDETACH\b",
        r"\bCREATE\b",
        r"\bSET\b",
        r"\bREMOVE\b",
        r"\bDROP\b",
        r"\bMERGE\b",
        r"\bLOAD\s+CSV\b",
        r"\bCALL\b",
        r"\bALTER\b",
        r"\bGRANT\b",
        r"\bREVOKE\b",
        r"\bDENY\b",
    ]

    for pattern in forbidden_patterns:
        if re.search(pattern, cleaned, flags=re.IGNORECASE):
            raise UnsafeCypherError(f"Prohibited operation detected in Cypher query: {pattern}")

    # Must contain MATCH or RETURN
    if not re.search(r"\bRETURN\b", cleaned, flags=re.IGNORECASE):
        raise UnsafeCypherError("Read-only Cypher query must include a RETURN clause.")

    return cleaned


def has_supporting_data(result: List[Dict[str, Any]]) -> bool:
    """
    Checks if a Cypher result contains grounded supporting data.
    """
    if not result:
        return False
    first = result[0]
    for _k, v in first.items():
        if isinstance(v, (int, float)) and v == 0:
            return False
    return True


def format_answer(result: List[Dict[str, Any]], grounded: bool) -> str:
    """
    Formats natural-language answer based on Cypher execution results.
    """
    if not grounded or not result:
        return "No matching rows found in the knowledge graph."

    first = result[0]
    for k, v in first.items():
        if isinstance(v, (int, float)):
            return f"Found {int(v)} rows matching your query."
    return f"Query returned: {result}"


def build_fallback_cypher(question: str, schema: Dict[str, Any]) -> Tuple[Optional[str], Dict[str, Any], Optional[str]]:
    """
    Deterministic rule-based fallback to construct safe parameterized Cypher queries.
    """
    properties = schema.get("properties", [])
    sample_values = schema.get("sample_values", {})
    lower_q = question.lower()

    # Total row count question
    if not properties or "total" in lower_q or "how many rows are there" in lower_q:
        return "MATCH (r:Row) RETURN count(r) AS count", {}, "total_count"

    # Match property and value
    matched_prop = None
    matched_val = None

    for prop, vals in sample_values.items():
        for v in vals:
            if v.lower() in lower_q:
                matched_prop = prop
                matched_val = v
                break
        if matched_val:
            break

    if not matched_prop:
        for prop in properties:
            if prop.lower() in lower_q:
                matched_prop = prop
                break

    if matched_prop and not matched_val:
        # Extract word after property
        match = re.search(rf"{matched_prop}\s*(?:is|=|belong to the|in|have)?\s*['\"]?([A-Za-z0-9_-]+)['\"]?", question, re.IGNORECASE)
        if match:
            matched_val = match.group(1)

    if not matched_prop and properties:
        matched_prop = properties[0]

    if matched_prop and matched_val:
        cypher = f"MATCH (r:Row) WHERE r.{matched_prop} = $value RETURN count(r) AS count"
        params = {"value": matched_val}
        return cypher, params, f"filter_{matched_prop}"

    if matched_prop:
        cypher = f"MATCH (r:Row) WHERE r.{matched_prop} IS NOT NULL RETURN count(r) AS count"
        return cypher, {}, f"filter_{matched_prop}_not_null"

    return "MATCH (r:Row) RETURN count(r) AS count", {}, "default_count"


class ChatService:
    UnsafeCypherError = UnsafeCypherError

    def __init__(self):
        self.settings = get_settings()
        self.UnsafeCypherError = UnsafeCypherError


    async def get_graph_schema(self) -> Dict[str, Any]:
        """
        Discovers dynamic properties and sample values present on Row nodes in Neo4j.
        """
        try:
            query = "MATCH (r:Row) RETURN keys(r) AS keys LIMIT 100"
            records = await neo4j_service.execute_query(query)

            all_keys = set()
            for rec in records:
                keys = rec.get("keys", [])
                for k in keys:
                    if k not in ["job_id", "dataset_id", "row_index"]:
                        all_keys.add(k)

            properties = sorted(list(all_keys))
            sample_values: Dict[str, List[str]] = {}

            for prop in properties:
                sample_query = f"MATCH (r:Row) WHERE r.{prop} IS NOT NULL RETURN DISTINCT r.{prop} AS val LIMIT 5"
                sample_records = await neo4j_service.execute_query(sample_query)
                sample_values[prop] = [str(r.get("val")) for r in sample_records if r.get("val") is not None]

            return {
                "properties": properties,
                "sample_values": sample_values,
            }
        except Exception as exc:
            logger.warning("Could not introspect Neo4j graph schema: %s", exc)
            return {"properties": [], "sample_values": {}}

    async def generate_cypher_llm(self, question: str, schema: Dict[str, Any]) -> str:
        """
        Generates Cypher query using OpenAI LLM if configured.
        """
        settings = get_settings()
        if not settings.OPENAI_API_KEY:
            return ""

        try:
            from openai import AsyncOpenAI
            client = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)

            prompt = f"""
            You are a Neo4j Cypher expert.
            Convert the following user question into a STRICTLY READ-ONLY Cypher query on node label `:Row`.
            
            Available dynamic properties: {schema.get('properties', [])}
            Sample values: {schema.get('sample_values', {})}
            
            Rules:
            1. Only return the Cypher query text, nothing else.
            2. Never use CREATE, DELETE, SET, DETACH, MERGE, or LOAD CSV.
            3. Always use parameters $value for literal filtering.
            
            Question: {question}
            """

            response = await client.chat.completions.create(
                model=settings.LLM_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
            )
            return response.choices[0].message.content.strip()
        except Exception as exc:
            logger.warning("LLM Cypher generation failed: %s", exc)
            return ""

    def validate_read_only_cypher(self, cypher: str) -> str:
        return validate_read_only_cypher(cypher)

    def has_supporting_data(self, result: List[Dict[str, Any]]) -> bool:
        return has_supporting_data(result)

    def format_answer(self, result: List[Dict[str, Any]], grounded: bool) -> str:
        return format_answer(result, grounded)

    def build_fallback_cypher(self, question: str, schema: Dict[str, Any]) -> Tuple[Optional[str], Dict[str, Any], Optional[str]]:
        return build_fallback_cypher(question, schema)


chat_service = ChatService()
