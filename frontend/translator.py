"""
Natural Language to Cypher Translation Layer (Rule-Based Pattern Matching)

Translates user questions in plain English into safe, parameterized Cypher queries
against dynamic Neo4j CSV schemas. Uses regex, tokenization, synonym mapping,
and fuzzy column matching.

Zero LLM calls required — deterministic, fast, safe, and inspectable.
"""

import re
import difflib
from typing import Any, Dict, List, Optional, Tuple


# =====================================================================
# Semantic Synonym Clusters for Dynamic Fuzzy Column Resolution
# =====================================================================

SYNONYM_CLUSTERS = [
    {"amount", "spending", "spend", "cost", "expense", "price", "total", "value", "payment", "charge", "fee", "balance", "transaction_amount"},
    {"customer", "customer_id", "cust_id", "client", "client_id", "user", "user_id", "account", "account_id", "buyer"},
    {"merchant", "vendor", "seller", "provider", "store", "shop", "company", "retailer", "merchant_name"},
    {"tx_id", "transaction", "transaction_id", "txn", "txn_id", "order", "order_id", "id"},
    {"category", "type", "group", "class", "department", "sector", "segment", "classification"},
    {"city", "location", "town", "metro", "place", "region", "country"},
    {"status", "state", "condition", "stage", "flag"},
    {"risk", "risk_score", "fraud_score", "risk_rating", "risk_level"}
]

DISTINCT_UNRESOLVED_CONCEPTS = {
    "credit_score": {"risk_score", "score", "amount"},
    "credit score": {"risk_score", "score", "amount"},
    "credit": {"risk_score", "merchant", "category", "amount"},
    "credit_rating": {"risk_score"},
    "credit rating": {"risk_score"},
    "fico": {"risk_score"},
    "fico_score": {"risk_score"},
    "interest_rate": {"risk_score", "amount"},
    "interest rate": {"risk_score", "amount"},
    "credit_limit": {"risk_score", "amount"},
    "credit limit": {"risk_score", "amount"},
    "salary": {"amount", "risk_score"},
    "wage": {"amount", "risk_score"},
    "gpa": {"risk_score", "amount"},
    "weather": {"city", "status"},
}


# =====================================================================
# Helper: Fuzzy Column Matching
# =====================================================================

def normalize_name(s: str) -> str:
    """Removes underscores, hyphens, and spaces for normalized comparison."""
    return re.sub(r"[_\-\s]+", "", s.lower())


def resolve_column_name(
    token_or_phrase: str,
    schema_columns: List[str],
    min_similarity: float = 0.78
) -> Optional[str]:
    """
    Fuzzy matches a token or phrase against schema columns using:
    1. Exact case-insensitive match
    2. Normalized match (ignoring underscores/hyphens)
    3. Substring containment (component-based)
    4. Bidirectional semantic synonym cluster lookup
    5. Sequence similarity ratio (difflib)
    """
    if not token_or_phrase or not schema_columns:
        return None

    cleaned_target = token_or_phrase.strip().lower()
    target_norm = normalize_name(cleaned_target)

    # 1. Exact case-insensitive match
    for col in schema_columns:
        if col.lower() == cleaned_target:
            return col

    # 2. Normalized match (e.g., 'riskscore' -> 'risk_score')
    for col in schema_columns:
        if normalize_name(col) == target_norm:
            return col

    # Guard: Do not confuse distinct concepts with schema columns
    forbidden_matches = DISTINCT_UNRESOLVED_CONCEPTS.get(cleaned_target, set())
    for k, v in DISTINCT_UNRESOLVED_CONCEPTS.items():
        if normalize_name(k) == target_norm:
            forbidden_matches = forbidden_matches.union(v)

    # 3. Substring containment (component-based)
    for col in schema_columns:
        col_lower = col.lower()
        if col_lower in forbidden_matches:
            continue
        if len(cleaned_target) >= 4 and len(col_lower) >= 4:
            if (cleaned_target in col_lower.split('_') or col_lower in cleaned_target.split('_') or
                col_lower.startswith(cleaned_target) or cleaned_target.startswith(col_lower)):
                return col

    # 4. Bidirectional synonym cluster lookup
    for cluster in SYNONYM_CLUSTERS:
        if cleaned_target in cluster or any(normalize_name(member) == target_norm for member in cluster):
            for col in schema_columns:
                col_clean = col.lower()
                col_norm = normalize_name(col_clean)
                if col_clean in forbidden_matches:
                    continue
                if col_clean in cluster or col_norm in cluster or any(normalize_name(m) == col_norm for m in cluster):
                    return col

    # 5. Fuzzy string distance (difflib)
    best_col = None
    best_score = 0.0

    for col in schema_columns:
        if col.lower() in forbidden_matches:
            continue
        # direct ratio
        r1 = difflib.SequenceMatcher(None, target_norm, normalize_name(col)).ratio()
        # token set ratio
        r2 = difflib.SequenceMatcher(None, cleaned_target, col.lower()).ratio()
        score = max(r1, r2)
        if score > best_score:
            best_score = score
            best_col = col

    if best_score >= min_similarity:
        return best_col

    return None


def extract_columns_from_text(text: str, schema_columns: List[str]) -> List[Tuple[str, str]]:
    """
    Scans question text for references to schema columns (direct or synonym).
    Returns list of (matched_column_in_schema, original_word_in_text).
    """
    matches = []
    # Tokenize into words and 2-word phrases
    words = re.findall(r"\b[a-zA-Z0-9_\-]+\b", text.lower())
    
    # Check 2-word phrases first
    for i in range(len(words) - 1):
        phrase = f"{words[i]} {words[i+1]}"
        col = resolve_column_name(phrase, schema_columns, min_similarity=0.75)
        if col and (col, phrase) not in matches:
            matches.append((col, phrase))

    # Check individual words
    for w in words:
        if len(w) <= 2:
            continue
        col = resolve_column_name(w, schema_columns, min_similarity=0.70)
        if col and not any(m[0] == col for m in matches):
            matches.append((col, w))

    return matches


# =====================================================================
# Operator and Value Parsers
# =====================================================================

OPERATOR_PATTERNS = [
    (r"(>=|\b(?:greater than or equal to|at least|no less than)\b)", ">="),
    (r"(<=|\b(?:less than or equal to|at most|no more than)\b)", "<="),
    (r"(>|\b(?:greater than|more than|higher than|above|over)\b)", ">"),
    (r"(<|\b(?:less than|fewer than|lower than|below|under)\b)", "<"),
    (r"(!=|<>|\b(?:not equal to|different from)\b)", "!="),
    (r"\b(contains|containing|with|has)\b", "CONTAINS"),
    (r"(==|=|\b(?:equal to|equals|is equal to|exactly|is)\b)", "="),
]


def parse_operator_and_value(text: str) -> Tuple[Optional[str], Optional[Any]]:
    """
    Extracts operator and target value from question text.
    Handles currency ($5000), percentages, decimals, integers, and quoted strings.
    """
    # 1. Quoted value check: e.g. "Apex" or 'SETTLED'
    quoted = re.search(r"['\"]([^'\"]+)['\"]", text)
    if quoted:
        val = quoted.group(1)
        # Check operator preceding it
        for pattern, op in OPERATOR_PATTERNS:
            if re.search(pattern, text[:quoted.start()], re.IGNORECASE):
                return op, val
        return "=", val

    # 2. Number extraction: handles $1,420.50 or 5000 or 0.50
    # Clean commas in numbers like 1,000
    cleaned_text = re.sub(r"(?<=\d),(?=\d)", "", text)
    num_match = re.search(r"(\$)?\s*(-?\d+(?:\.\d+)?)", cleaned_text)
    
    if num_match:
        num_str = num_match.group(2)
        val = float(num_str) if "." in num_str else int(num_str)
        # Check operator
        prefix = cleaned_text[:num_match.start()]
        for pattern, op in OPERATOR_PATTERNS:
            if re.search(pattern, prefix, re.IGNORECASE):
                return op, val
        return "=", val

    # 3. Trailing word value: e.g. "status is SETTLED" or "category is Infrastructure"
    for pattern, op in OPERATOR_PATTERNS:
        match = re.search(rf"{pattern}\s+([A-Za-z0-9_\-]+)", text, re.IGNORECASE)
        if match:
            raw_val = match.group(2)
            if raw_val.lower() not in {"the", "a", "an", "all", "rows", "records", "any"}:
                return op, raw_val

    return None, None


# =====================================================================
# Main Translation Function
# =====================================================================

def translate_question(question: str, schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Translates a plain-English user question into a parameterized Cypher query
    against the provided dynamic graph schema.
    
    Args:
        question: User query string
        schema: Graph schema dictionary containing:
                - 'row_properties' or 'columns': list of dynamic CSV column names
                - 'target_dataset_id' (optional): ID of active dataset
                
    Returns:
        {"matched": True, "cypher": "...", "params": {...}, "template_used": "..."}
        or
        {"matched": False, "reason": "..."}
    """
    if not question or not question.strip():
        return {"matched": False, "reason": "Question string is empty."}

    q = question.strip()
    q_lower = q.lower()

    # Extract schema columns
    cols = schema.get("row_properties") or schema.get("columns") or []
    dataset_id = schema.get("target_dataset_id")

    # -----------------------------------------------------------------
    # Pattern 5: Dataset Overview
    # e.g., "what data do you have", "what can I ask about", "describe this dataset"
    # -----------------------------------------------------------------
    overview_triggers = [
        "what data do you have",
        "what data is this",
        "what can i ask",
        "what can i ask about",
        "describe this dataset",
        "describe the dataset",
        "dataset overview",
        "show dataset summary",
        "what columns exist",
        "what columns are there",
        "tell me about the data",
        "show schema",
        "help me understand",
        "explain the dataset"
    ]
    if any(trig in q_lower for trig in overview_triggers) or q_lower in {"describe", "schema", "overview", "help"}:
        cypher = """
        MATCH (d:Dataset)
        WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
        OPTIONAL MATCH (d)-[:HAS_ROW]->(r:Row)
        WITH d, count(r) AS actual_row_count, collect(keys(r)) AS all_keys
        UNWIND (CASE WHEN size(all_keys) = 0 THEN [[]] ELSE all_keys END) AS key_list
        UNWIND (CASE WHEN size(key_list) = 0 THEN [null] ELSE key_list END) AS k
        WITH d, actual_row_count, collect(DISTINCT k) AS columns
        RETURN d.id AS dataset_id,
               d.name AS dataset_name,
               coalesce(d.row_count, actual_row_count) AS registered_rows,
               actual_row_count AS actual_rows,
               [col IN columns WHERE col IS NOT NULL AND col <> 'row_index'] AS columns
        """.strip()
        return {
            "matched": True,
            "cypher": cypher,
            "params": {"dataset_id": dataset_id},
            "template_used": "describe_dataset"
        }

    # -----------------------------------------------------------------
    # Pattern 4: Entity-Specific Lookups
    # e.g., "what is the total for customer CUST-8912"
    # e.g., "total spending for merchant Delta Air Lines"
    # e.g., "show rows for tx TX-104"
    # -----------------------------------------------------------------
    # Detect entity ID patterns like CUST-8912, TX-101, ORD-404, or alphanumeric IDs
    entity_id_match = re.search(r"\b([A-Z]{2,6}[-_]\d{2,10})\b", q)
    if entity_id_match:
        entity_val = entity_id_match.group(1)
        # Determine likely identifier column
        prefix = entity_id_match.group(1).split("-")[0].split("_")[0].lower()
        target_col = None
        for col in cols:
            if prefix in col.lower() or "id" in col.lower():
                target_col = col
                break
        if not target_col:
            target_col = cols[0] if cols else "id"

        # Check if question is asking for aggregation or rows
        is_agg = any(term in q_lower for term in ["total", "sum", "spending", "amount", "average", "avg"])
        if is_agg:
            # Find numeric column to aggregate
            num_col = resolve_column_name("amount", cols) or "amount"
            cypher = """
            MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
            WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
              AND r[$filter_prop] = $filter_val
              AND r[$sum_prop] IS NOT NULL
            RETURN r[$filter_prop] AS group_key,
                   sum(toFloat(r[$sum_prop])) AS total_sum,
                   count(r) AS row_count
            """.strip()
            return {
                "matched": True,
                "cypher": cypher,
                "params": {
                    "dataset_id": dataset_id,
                    "sum_prop": num_col,
                    "group_prop": target_col,
                    "filter_prop": target_col,
                    "filter_val": entity_val
                },
                "template_used": "sum_property_grouped_by"
            }
        else:
            # Row lookup
            cypher = """
            MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
            WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
              AND r[$prop_name] = $val
            RETURN r.row_index AS row_index, properties(r) AS row_data
            ORDER BY r.row_index ASC
            LIMIT $limit
            """.strip()
            return {
                "matched": True,
                "cypher": cypher,
                "params": {
                    "dataset_id": dataset_id,
                    "prop_name": target_col,
                    "val": entity_val,
                    "limit": 25
                },
                "template_used": "find_rows_where"
            }

    # Detect possessive entity lookup, e.g. "What is Delta's total spending"
    possessive_match = re.search(r"(?:what is|show me|find|get)?\s*([A-Za-z0-9&]+(?: [A-Za-z0-9&]+)?)'s\s+(total\s+)?(spending|amount|spend|cost)\b", q, re.IGNORECASE)
    if possessive_match:
        raw_entity = possessive_match.group(1).strip()
        if raw_entity.lower() not in {"each", "all", "every", "category", "merchant", "customer", "vendor"}:
            filter_col = resolve_column_name("merchant", cols) or "merchant"
            num_col = resolve_column_name("amount", cols) or "amount"
            cypher = """
            MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
            WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
              AND toLower(toString(r[$filter_prop])) CONTAINS toLower($filter_val)
              AND r[$sum_prop] IS NOT NULL
            RETURN r[$filter_prop] AS group_key,
                   sum(toFloat(r[$sum_prop])) AS total_sum,
                   count(r) AS row_count
            """.strip()
            return {
                "matched": True,
                "cypher": cypher,
                "params": {
                    "dataset_id": dataset_id,
                    "sum_prop": num_col,
                    "group_prop": filter_col,
                    "filter_prop": filter_col,
                    "filter_val": raw_entity
                },
                "template_used": "sum_property_grouped_by"
            }

    # Also detect entity lookup by specific named entity with "for [EntityName]"
    for_entity_match = re.search(r"\bfor\s+(merchant|customer|client|vendor|category)?\s*([\"']?[A-Za-z0-9\s&]+[\"']?)", q, re.IGNORECASE)
    if for_entity_match and any(w in q_lower for w in ["total", "sum", "spending"]):
        raw_entity = for_entity_match.group(2).strip().strip("'\"")
        # Ensure it's not a generic word
        if raw_entity.lower() not in {"each", "all", "every", "category", "merchant"}:
            entity_type = for_entity_match.group(1)
            filter_col = resolve_column_name(entity_type if entity_type else "merchant", cols)
            num_col = resolve_column_name("amount", cols) or "amount"
            if filter_col:
                cypher = """
                MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
                WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
                  AND toLower(toString(r[$filter_prop])) = toLower($filter_val)
                  AND r[$sum_prop] IS NOT NULL
                RETURN r[$filter_prop] AS group_key,
                       sum(toFloat(r[$sum_prop])) AS total_sum,
                       count(r) AS row_count
                """.strip()
                return {
                    "matched": True,
                    "cypher": cypher,
                    "params": {
                        "dataset_id": dataset_id,
                        "sum_prop": num_col,
                        "group_prop": filter_col,
                        "filter_prop": filter_col,
                        "filter_val": raw_entity
                    },
                    "template_used": "sum_property_grouped_by"
                }

    # -----------------------------------------------------------------
    # Pattern 1: Counting Questions
    # e.g., "how many rows...", "how many X are there", "count of..."
    # -----------------------------------------------------------------
    is_counting = any(re.search(p, q_lower) for p in [
        r"^how many\b",
        r"\bhow many\b",
        r"^count of\b",
        r"\bcount of\b",
        r"^number of\b",
        r"\bnumber of\b"
    ])

    if is_counting:
        # Check if counting all rows (only when no filter condition is present)
        has_filter = any(w in q_lower for w in ["where", "with", "have", "that are", "which are", "equal", "="])
        if not has_filter and re.search(r"\b(how many|count of|number of)\s+(total\s+)?(rows|records|items|entries|all)\b", q_lower):
            cypher = """
            MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
            WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
            RETURN count(r) AS count
            """.strip()
            return {
                "matched": True,
                "cypher": cypher,
                "params": {"dataset_id": dataset_id},
                "template_used": "count_rows_matching"
            }

        # Filtered count: check for property = value
        # e.g. "how many transactions are SETTLED", "count of rows where status is FLAGGED"
        target_prop = None
        target_val = None

        # Check explicit "where X = Y" or "with X equal to Y" or "where X is Y"
        where_match = re.search(r"\b(?:where|with|have)\s+([a-zA-Z_]+)\s*(?:is|=|==|equals)\s*['\"]?([a-zA-Z0-9_\-]+)['\"]?", q, re.IGNORECASE)
        if where_match:
            prop_candidate = resolve_column_name(where_match.group(1), cols)
            if prop_candidate:
                target_prop = prop_candidate
                target_val = where_match.group(2).strip()

        # Check adjective/value patterns: e.g. "how many settled transactions"
        if not target_prop:
            for word in re.findall(r"\b[A-Za-z0-9_\-]+\b", q):
                if word.lower() in {"how", "many", "rows", "are", "there", "transactions", "records", "items", "is", "the", "a", "an", "of", "count"}:
                    continue
                # Could this word be a column value? Check status, category, etc.
                # If there's a column named 'status' or 'category', match it
                status_col = resolve_column_name("status", cols)
                if status_col:
                    target_prop = status_col
                    target_val = word.upper()
                    break

        if target_prop and target_val:
            cypher = """
            MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
            WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
              AND toLower(toString(r[$prop_name])) = toLower($prop_val)
            RETURN count(r) AS count, $prop_name AS property, $prop_val AS matching_value
            """.strip()
            return {
                "matched": True,
                "cypher": cypher,
                "params": {
                    "dataset_id": dataset_id,
                    "prop_name": target_prop,
                    "prop_val": target_val
                },
                "template_used": "count_rows_matching"
            }

        # Fallback to general count
        cypher = """
        MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
        WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
        RETURN count(r) AS count
        """.strip()
        return {
            "matched": True,
            "cypher": cypher,
            "params": {"dataset_id": dataset_id},
            "template_used": "count_rows_matching"
        }

    # -----------------------------------------------------------------
    # Pattern 2: Aggregation Questions
    # e.g., "total/sum of X", "average X", "what is the total spending for Y"
    # -----------------------------------------------------------------
    is_aggregation = any(re.search(p, q_lower) for p in [
        r"\b(total|sum|spending|average|avg)\b",
        r"\bspend\b"
    ])

    if is_aggregation:
        # 1. Identify sum column (fuzzy)
        # e.g. "spending", "amount", "cost", "risk_score"
        sum_col = None
        for word in ["spending", "spend", "amount", "total", "cost", "expense", "fee", "risk_score", "score"]:
            if word in q_lower:
                sum_col = resolve_column_name(word, cols)
                if sum_col:
                    break
        if not sum_col:
            sum_col = resolve_column_name("amount", cols)

        # 2. Identify grouping column
        # e.g. "grouped by category", "by merchant", "per city"
        group_col = None
        by_match = re.search(r"\b(?:grouped by|by|per|across|for each)\s+([a-zA-Z_]+)\b", q_lower)
        if by_match:
            group_candidate = by_match.group(1).strip()
            group_col = resolve_column_name(group_candidate, cols)

        # If no grouping column specified, check if another column exists in question
        if not group_col:
            for c in cols:
                if c != sum_col and c.lower() in q_lower:
                    group_col = c
                    break

        # If still no grouping column, fallback to primary categorical column (e.g., category, merchant, city)
        if not group_col:
            for fallback in ["category", "merchant", "city", "status"]:
                candidate = resolve_column_name(fallback, cols)
                if candidate:
                    group_col = candidate
                    break

        if sum_col and group_col:
            cypher = """
            MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
            WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
              AND ($filter_prop IS NULL OR r[$filter_prop] = $filter_val)
              AND r[$sum_prop] IS NOT NULL
            RETURN r[$group_prop] AS group_key,
                   sum(toFloat(r[$sum_prop])) AS total_sum,
                   count(r) AS row_count
            ORDER BY total_sum DESC
            """.strip()
            return {
                "matched": True,
                "cypher": cypher,
                "params": {
                    "dataset_id": dataset_id,
                    "sum_prop": sum_col,
                    "group_prop": group_col,
                    "filter_prop": None,
                    "filter_val": None
                },
                "template_used": "sum_property_grouped_by"
            }

    # -----------------------------------------------------------------
    # Pattern 3: Filter / Lookup Questions
    # e.g., "rows where X > N", "which rows have X equal to Y", "show me X above $5000"
    # -----------------------------------------------------------------
    is_filter = any(re.search(p, q_lower) for p in [
        r"\b(rows|records|transactions|items)\s+where\b",
        r"\bwhich\s+(rows|records|transactions)\b",
        r"\bshow\s+(me\s+)?(all\s+)?(rows|records|transactions)\b",
        r"\bfind\s+(all\s+)?(rows|records|transactions)\b",
        r"\bwhere\b",
        r"\babove\b",
        r"\bbelow\b",
        r"\bgreater than\b",
        r"\bless than\b",
        r"\bcontains\b"
    ])

    if is_filter:
        op, val = parse_operator_and_value(q)
        if op and val is not None:
            # Determine which column the condition applies to
            target_col = None

            # 1. Check if query specifies an explicit column name before the operator
            explicit_prop_match = re.search(r"\b(?:where|filter|have|with)?\s*([a-zA-Z0-9_\-]+)\s*(?:>=|<=|>|<|!=|=|==|contains|is|\babove\b|\bbelow\b)", q, re.IGNORECASE)
            if explicit_prop_match:
                candidate_word = explicit_prop_match.group(1).strip()
                if candidate_word.lower() not in {"rows", "records", "transactions", "items", "entries", "all", "where", "filter", "show", "find", "me"}:
                    resolved = resolve_column_name(candidate_word, cols)
                    if resolved:
                        target_col = resolved
                    else:
                        # User specified a column name that does not exist in schema — do not force a low-confidence guess
                        return {
                            "matched": False,
                            "reason": f"Column '{candidate_word}' does not exist in dataset schema: {cols}"
                        }

            # 2. Check for column mentioned anywhere in query text
            if not target_col:
                for c in cols:
                    if c.lower() in q_lower or normalize_name(c) in normalize_name(q):
                        target_col = c
                        break

            # 3. Only infer if strong contextual evidence exists (e.g. currency symbol '$' for amount)
            if not target_col:
                if "$" in q or any(w in q_lower for w in ["dollar", "dollars", "usd", "price", "spending", "cost"]):
                    target_col = resolve_column_name("amount", cols)
                elif isinstance(val, (int, float)) and val < 1.0 and any(w in q_lower for w in ["risk", "fraud", "score"]):
                    target_col = resolve_column_name("risk_score", cols)

            if target_col:
                # Format condition
                if op in {">", "<", ">=", "<="}:
                    condition = f"toFloat(r[$prop_name]) {op} toFloat($val)"
                elif op == "CONTAINS":
                    condition = "toLower(toString(r[$prop_name])) CONTAINS toLower(toString($val))"
                elif op in {"!=", "<>"}:
                    condition = "toLower(toString(r[$prop_name])) <> toLower(toString($val))"
                else:
                    condition = "toLower(toString(r[$prop_name])) = toLower(toString($val))"

                cypher = f"""
                MATCH (d:Dataset)-[:HAS_ROW]->(r:Row)
                WHERE ($dataset_id IS NULL OR d.id = $dataset_id)
                  AND {condition}
                RETURN r.row_index AS row_index, properties(r) AS row_data
                ORDER BY r.row_index ASC
                LIMIT $limit
                """.strip()

                return {
                    "matched": True,
                    "cypher": cypher,
                    "params": {
                        "dataset_id": dataset_id,
                        "prop_name": target_col,
                        "val": val,
                        "limit": 25
                    },
                    "template_used": "find_rows_where"
                }

    # -----------------------------------------------------------------
    # No confident match found
    # -----------------------------------------------------------------
    return {
        "matched": False,
        "reason": f"No supported question pattern or recognized columns detected with sufficient confidence in: '{question}'."
    }
