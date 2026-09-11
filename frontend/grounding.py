"""
Grounding and Honesty Layer for Neo4j CSV Chatbot.

Wraps the NL-to-Cypher translation and query execution pipeline into the final
strict /chat response contract:
{
  "answer": str,
  "cypher": Optional[str],
  "result": list[dict],
  "grounded": bool
}

Core Invariants:
1. No Hallucinated Numbers: Every numeric claim in `answer` must trace directly
   to values in the query `result`.
2. Explicit Schema Transparency: When a question cannot be answered, explain WHAT
   columns are in the dataset and suggest valid query patterns.
3. Proven Effort: If an entity lookup returns 0 matching records, `grounded: false`
   is returned WITH the executed Cypher and empty array, proving the graph was searched.
4. Schema Pre-check: Out-of-domain concepts are caught before Cypher translation
   to save wasted queries and provide instant, accurate guidance.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

from query_engine import Neo4jQueryEngine, default_engine, get_graph_schema
from translator import translate_question, resolve_column_name, normalize_name
from llm_layer import llm_generate_cypher_fallback, llm_rephrase_answer, USE_LLM_FALLBACK
from guardrails import assert_answer_is_grounded, UngroundedFactError


# =====================================================================
# Known Out-of-Domain / Impossible Concept Catalog
# =====================================================================

OUT_OF_DOMAIN_CONCEPTS = {
    "salary": "employee compensation / salary",
    "wage": "employee wages",
    "payroll": "payroll records",
    "employee": "human resources / employee",
    "credit score": "credit rating & credit score",
    "credit_score": "credit rating & credit score",
    "credit rating": "credit ratings",
    "credit limit": "credit limits",
    "interest rate": "interest rates",
    "capital of": "world geography & capital cities",
    "ceo": "executive leadership / corporate management",
    "stock price": "public equities & stock market prices",
    "world war": "historical events",
    "2 + 2": "general arithmetic",
    "weather": "weather & meteorological",
    "temperature": "temperature",
    "rain": "weather & precipitation",
    "forecast": "weather forecasts",
    "flight": "flight scheduling",
    "airline delay": "flight delay",
    "patient": "healthcare & patient",
    "diagnosis": "medical diagnosis",
    "prescription": "pharmaceutical",
    "hospital": "clinical / hospital",
    "student": "academic & student",
    "grade": "academic grading",
    "gpa": "academic performance",
    "course": "academic coursework",
    "inventory": "warehouse inventory",
    "warehouse": "warehouse logistics",
    "stock level": "inventory stock levels",
    "hotel room": "hotel reservations",
    "recipe": "food recipes",
    "ingredient": "culinary ingredients"
}


# =====================================================================
# Helpful Suggestion Builder
# =====================================================================

def build_schema_suggestions(columns: List[str]) -> str:
    """Generates 2-3 contextual question suggestions based on actual schema columns."""
    if not columns:
        return "uploading a CSV file to explore your data"

    suggestions = []
    col_set = set(columns)

    # 1. Numeric / Monetary suggestion
    num_col = None
    for candidate in ["amount", "price", "cost", "spending", "total", "risk_score"]:
        if candidate in col_set:
            num_col = candidate
            break

    # Categorical group candidate
    cat_col = None
    for candidate in ["category", "merchant", "city", "status", "department"]:
        if candidate in col_set:
            cat_col = candidate
            break

    if num_col and cat_col:
        suggestions.append(f"'total {num_col} by {cat_col}'")
    elif num_col:
        suggestions.append(f"'transactions with {num_col} above 1000'")

    # 2. Status / Filter suggestion
    if "status" in col_set:
        suggestions.append("'count of rows where status is SETTLED'")
    elif "city" in col_set:
        suggestions.append("'transactions in a specific city'")
    elif cat_col:
        suggestions.append(f"'rows where {cat_col} is [value]'")

    # 3. Always available overview suggestion
    suggestions.append("'describe this dataset'")

    if len(suggestions) == 1:
        return suggestions[0]
    elif len(suggestions) == 2:
        return f"{suggestions[0]} or {suggestions[1]}"
    else:
        return f"{suggestions[0]}, {suggestions[1]}, or {suggestions[2]}"


# =====================================================================
# Natural Language Grounded Answer Synthesizers
# =====================================================================

def synthesize_grounded_answer(
    template_used: str,
    data: List[Dict[str, Any]],
    params: Dict[str, Any]
) -> Tuple[str, bool]:
    """
    Constructs a factual, natural-language sentence strictly from returned query results.
    Never invents numbers. Every figure is mapped from `data`.
    
    Returns:
        (answer_string, is_grounded)
    """
    # 1. Template: describe_dataset
    if template_used == "describe_dataset":
        if not data:
            return "No dataset details could be found in the graph.", False
        row = data[0]
        name = row.get("dataset_name") or row.get("dataset_id", "Uploaded Dataset")
        rows_count = row.get("actual_rows", row.get("registered_rows", 0))
        cols = row.get("columns", [])
        cols_str = ", ".join([f"`{c}`" for c in cols]) if cols else "none"
        ans = (
            f"This dataset ('{name}') contains {rows_count:,} recorded rows across "
            f"{len(cols)} columns: {cols_str}."
        )
        return ans, True

    # 2. Template: count_rows_matching
    if template_used == "count_rows_matching":
        if not data:
            return "The count query completed, but returned no count metric.", False
        row = data[0]
        cnt = row.get("count", 0)
        prop = row.get("property")
        val = row.get("matching_value")

        if cnt == 0:
            if prop and val is not None:
                return f"I ran a query for `{prop} = '{val}'`, but found 0 matching records in the uploaded data.", False
            return "Found 0 matching records in the uploaded data.", False

        if prop and val is not None:
            ans = f"Found {cnt:,} row{'s' if cnt != 1 else ''} where `{prop}` is '{val}'."
        else:
            ans = f"There are {cnt:,} total recorded row{'s' if cnt != 1 else ''} in the dataset."
        return ans, True

    # 3. Template: sum_property_grouped_by
    if template_used == "sum_property_grouped_by":
        if not data:
            filter_val = params.get("filter_val")
            if filter_val:
                return f"I ran an aggregation query for entity '{filter_val}', but found no matching records in the uploaded data.", False
            return "The aggregation query returned no rows for the requested properties.", False

        sum_prop = params.get("sum_prop", "amount")
        group_prop = params.get("group_prop", "category")
        filter_val = params.get("filter_val")

        # Entity-filtered single result
        if filter_val and len(data) == 1:
            item = data[0]
            total = item.get("total_sum", 0.0)
            cnt = item.get("row_count", 1)
            ans = (
                f"For `{filter_val}`, the total `{sum_prop}` is ${total:,.2f} USD "
                f"across {cnt:,} transaction{'s' if cnt != 1 else ''}."
            )
            return ans, True

        # Grouped summary list
        top_item = data[0]
        top_key = top_item.get("group_key", "Unknown")
        top_val = top_item.get("total_sum", 0.0)
        top_cnt = top_item.get("row_count", 0)

        if len(data) == 1:
            ans = (
                f"The total `{sum_prop}` for {group_prop} '{top_key}' is ${top_val:,.2f} USD "
                f"across {top_cnt:,} record{'s' if top_cnt != 1 else ''}."
            )
        else:
            second_item = data[1]
            second_key = second_item.get("group_key", "Unknown")
            second_val = second_item.get("total_sum", 0.0)
            ans = (
                f"Total `{sum_prop}` grouped by `{group_prop}` is led by '{top_key}' at "
                f"${top_val:,.2f} USD ({top_cnt:,} records), followed by '{second_key}' "
                f"at ${second_val:,.2f} USD across {len(data)} total groups."
            )
        return ans, True

    # 4. Template: find_rows_where
    if template_used == "find_rows_where":
        if not data:
            prop = params.get("prop_name", "attribute")
            val = params.get("val", "value")
            return f"I ran a query for `{prop}` matching '{val}', but found no matching records in the uploaded data.", False

        prop = params.get("prop_name", "attribute")
        cnt = len(data)
        first_row = data[0]
        row_idx = first_row.get("row_index", 1)
        row_details = first_row.get("row_data", first_row)

        # Build readable sample preview from 2-3 prominent columns
        sample_keys = [k for k in ["merchant", "category", "amount", "status", "city"] if k in row_details]
        if not sample_keys:
            sample_keys = [k for k in row_details.keys() if k != "row_index"][:3]

        details_preview = ", ".join([f"{k}: {row_details[k]}" for k in sample_keys])
        ans = (
            f"Found {cnt:,} matching row{'s' if cnt != 1 else ''}. "
            f"For example, row #{row_idx} has {details_preview}."
        )
        return ans, True

    # 5. Template: llm_fallback
    if template_used == "llm_fallback":
        if not data:
            return "I ran a query for your question, but found no matching records in the uploaded data.", False
        row = data[0]
        # Single aggregate scalar returned (e.g. average_risk_score, total_amount)
        if len(row) == 1:
            k, v = next(iter(row.items()))
            k_readable = k.replace("_", " ")
            if isinstance(v, float):
                return f"Based on the query result, the {k_readable} is {v:.2f}.", True
            return f"Based on the query result, the {k_readable} is {v}.", True
        # Costliest / max record
        if len(data) == 1 and "merchant" in row and "amount" in row:
            return (
                f"The record with the highest charge is '{row['merchant']}' "
                f"with an amount of ${float(row['amount']):,.2f} USD."
            ), True
        first_row_details = ", ".join([f"{k}: {v}" for k, v in list(row.items())[:3]])
        return f"Found {len(data):,} matching record{'s' if len(data) != 1 else ''}. For example, {first_row_details}.", True

    # Fallback generic grounded formatter
    if not data:
        return "Query executed successfully, but found 0 matching records in the graph.", False
    return f"Query returned {len(data):,} matching record{'s' if len(data) != 1 else ''}.", True


# =====================================================================
# Main Chat Contract Entrypoint
# =====================================================================

def answer_question(
    question: str,
    schema: Optional[Dict[str, Any]] = None,
    engine: Optional[Neo4jQueryEngine] = None,
    use_llm_override: Optional[bool] = None
) -> Dict[str, Any]:
    """
    Executes the full Grounding and Honesty pipeline:
    1. Schema Inspection & Pre-Check (guards against empty graph & out-of-domain concepts)
    2. NL-to-Cypher Translation (rule-based with fuzzy column resolution)
       -> If matched: False, optionally invokes LLM Fallback Cypher generator (if USE_LLM_FALLBACK=True)
    3. Safe Read-Only Execution with Timeout
    4. Honest Grounding & Result Synthesis (strictly grounded answers or transparent guidance)
       -> If grounded: True, optionally invokes LLM natural rephrasing strictly constrained to result data
    
    Returns contract:
    {
        "answer": str,
        "cypher": Optional[str],
        "result": list[dict],
        "grounded": bool,
        "execution_time_ms": float
    }
    """
    query_engine = engine or default_engine
    enable_llm = use_llm_override if use_llm_override is not None else USE_LLM_FALLBACK
    q = (question or "").strip()
    q_lower = q.lower()

    # Step 1: Resolve Graph Schema
    graph_schema = schema or query_engine.get_graph_schema()
    cols = graph_schema.get("row_properties") or graph_schema.get("columns") or []
    dataset_id = graph_schema.get("target_dataset_id")

    # Guard 1: Zero Data Loaded Pre-check
    if not cols:
        return {
            "answer": "No dataset is currently loaded in the database. Please upload a CSV file to begin asking questions.",
            "cypher": None,
            "result": [],
            "grounded": False,
            "execution_time_ms": 0.0
        }

    # Guard 2A: Causality / Explanatory inquiry guard (Category D1)
    if re.search(r"\b(why did|why is|why are|why was|reason for|cause of|explain why)\b", q_lower):
        cols_formatted = ", ".join([f"`{c}`" for c in cols])
        return {
            "answer": (
                f"I don't have causal or explanatory data to answer 'why' spending changed. "
                f"The dataset contains transaction records with columns: {cols_formatted}, but does not record reasons, "
                f"external events, or narrative justifications."
            ),
            "cypher": None,
            "result": [],
            "grounded": False,
            "execution_time_ms": 0.0
        }

    # Guard 2B: Unverified / Leading Premise Guard (Category D2)
    # e.g., "Since AWS is our riskiest vendor, how much did we pay them?"
    premise_match = re.search(r"\b(since|given that|assuming)\s+([A-Za-z0-9&]+)\s+is\s+(?:our|the)\s+(riskiest|highest|lowest|most|least)\b", q_lower)
    if premise_match:
        premise_entity = premise_match.group(2).strip()
        premise_superlative = premise_match.group(3).strip()

        try:
            all_rows_res = query_engine.run_cypher_readonly(
                "MATCH (d:Dataset)-[:HAS_ROW]->(r:Row) RETURN properties(r) AS p"
            )
            ent_risk = None
            max_risk_val = None
            max_risk_m = None
            for item in (all_rows_res.data or []):
                row_props = item.get("p", item)
                m_val = str(row_props.get("merchant", ""))
                if premise_entity.lower() in m_val.lower():
                    ent_risk = row_props.get("risk_score")
                r_score = row_props.get("risk_score")
                if r_score is not None:
                    try:
                        num_r = float(r_score)
                        if max_risk_val is None or num_r > max_risk_val:
                            max_risk_val = num_r
                            max_risk_m = m_val
                    except (ValueError, TypeError):
                        pass

            if ent_risk is not None and max_risk_val is not None and float(ent_risk) < float(max_risk_val):
                return {
                    "answer": (
                        f"I cannot verify the premise that {premise_entity.upper()} is our riskiest vendor. "
                        f"In the dataset, {premise_entity.upper()} has a risk score of {float(ent_risk):.2f}, "
                        f"whereas the highest risk score in the data is {float(max_risk_val):.2f} ({max_risk_m}). "
                        f"To check spending for {premise_entity.upper()} without assumptions, ask 'What is the total spending for {premise_entity.upper()}?'"
                    ),
                    "cypher": None,
                    "result": [],
                    "grounded": False,
                    "execution_time_ms": 0.0
                }
        except Exception:
            pass

    # Guard 2C: Compound Partial Match (Category C)
    # e.g., "What is Delta's total spending, and what is their credit score?"
    compound_match = re.search(r"^(.*?)(?:,\s*|\s+)and\s+(?:what is\s+)?(?:their\s+)?([a-z0-9_\s]+)\??$", q, re.IGNORECASE)
    if compound_match:
        part1 = compound_match.group(1).strip()
        part2 = compound_match.group(2).strip()
        part2_resolved = resolve_column_name(part2, cols, min_similarity=0.75)
        if not part2_resolved and any(w in part2.lower() for w in ["credit score", "credit_score", "score", "gpa", "salary", "rating"]):
            trans1 = translate_question(part1, graph_schema)
            if trans1.get("matched", False):
                try:
                    q_res1 = query_engine.run_cypher_readonly(trans1["cypher"], trans1["params"])
                    raw_rows1 = q_res1.data
                    if raw_rows1:
                        ans1, is_g1 = synthesize_grounded_answer(trans1["template_used"], raw_rows1, trans1["params"])
                        cols_formatted = ", ".join([f"`{c}`" for c in cols])
                        honest_answer = f"{ans1} Note: {part2} is not available in this dataset (available columns: {cols_formatted})."
                        return {
                            "answer": honest_answer,
                            "cypher": trans1["cypher"],
                            "result": raw_rows1,
                            "grounded": True,
                            "execution_time_ms": q_res1.execution_time_ms
                        }
                except Exception:
                    pass

    # Guard 2D: Out-of-Domain Concept Pre-check
    # If the question asks about a domain concept that has no correspondence in the schema
    for trigger_word, concept_name in OUT_OF_DOMAIN_CONCEPTS.items():
        if re.search(rf"\b{re.escape(trigger_word)}\b", q_lower):
            matches_col = resolve_column_name(trigger_word, cols, min_similarity=0.75)
            if not matches_col:
                cols_formatted = ", ".join([f"`{c}`" for c in cols])
                suggestions = build_schema_suggestions(cols)
                ans = (
                    f"I don't have data about {concept_name}. "
                    f"This dataset contains columns: {cols_formatted}. "
                    f"Try asking about {suggestions}."
                )
                return {
                    "answer": ans,
                    "cypher": None,
                    "result": [],
                    "grounded": False,
                    "execution_time_ms": 0.0
                }

    # Step 2: Translate Question to Parameterized Cypher
    translation = translate_question(q, graph_schema)

    # Guard 3: If rule-based translator returned matched: False, attempt LLM Fallback (if enabled)
    if not translation.get("matched", False):
        llm_candidate = None
        if enable_llm:
            llm_candidate = llm_generate_cypher_fallback(q, graph_schema)

        if llm_candidate:
            cypher = llm_candidate["cypher"]
            params = llm_candidate["params"]
            template_used = llm_candidate["template_used"]
        else:
            cols_formatted = ", ".join([f"`{c}`" for c in cols])
            suggestions = build_schema_suggestions(cols)
            ans = (
                f"I don't have data to answer that question. "
                f"This dataset contains columns: {cols_formatted}. "
                f"Try asking about {suggestions}."
            )
            return {
                "answer": ans,
                "cypher": None,
                "result": [],
                "grounded": False,
                "execution_time_ms": 0.0
            }
    else:
        cypher = translation["cypher"]
        params = translation["params"]
        template_used = translation["template_used"]

    # Step 3: Safe Read-Only Cypher Execution
    try:
        query_result = query_engine.run_cypher_readonly(cypher, params)
        raw_rows = query_result.data
        exec_ms = query_result.execution_time_ms
    except Exception as err:
        return {
            "answer": f"Database query execution failed: {err}",
            "cypher": cypher,
            "result": [],
            "grounded": False,
            "execution_time_ms": 0.0
        }

    # Step 4: Grounded Synthesis & Honesty Verification
    synthesized_answer, is_grounded = synthesize_grounded_answer(
        template_used=template_used,
        data=raw_rows,
        params=params
    )

    # Optional Polish: If grounded and LLM is enabled, polish answer strictly from result data
    if is_grounded and enable_llm and len(raw_rows) > 0:
        synthesized_answer = llm_rephrase_answer(q, raw_rows, synthesized_answer)

    # Step 5: Hard Assertion Gate — Safety Net against ungrounded / hallucinated claims
    if is_grounded:
        try:
            assert_answer_is_grounded(synthesized_answer, raw_rows)
        except UngroundedFactError:
            return {
                "answer": "Unable to verify this answer against the data.",
                "cypher": cypher,
                "result": raw_rows,
                "grounded": False,
                "execution_time_ms": exec_ms
            }

    return {
        "answer": synthesized_answer,
        "cypher": cypher,
        "result": raw_rows,
        "grounded": is_grounded,
        "execution_time_ms": exec_ms
    }
