# Graph Observatory — CSV to Neo4j Hackathon Frontend

A streamlined, high-density data observatory frontend for taking CSV files, streaming them into Neo4j via Kafka, and asking plain-English questions through a grounded Cypher chatbot.

## Header Structure
The header is clean and focused:
- **Brand/Wordmark**: `Axiom·Graph` telemetry glyph on the left.
- **4-Stage Tab Navigation**: `Upload`  `Preview`  `Loading`  `Chat` on the right.
- **Freely Clickable**: Each stage can be clicked at any time to switch views, functioning like tabs with checkmarks indicating completed milestones.

## 4 Distinct Stages & Empty States
1. **`Upload`**:
   - Drag-and-drop zone + "Browse files" fallback.
   - Non-CSV and empty file rejection.
   - 1-click sample dataset loader.
   - Selecting a file advances to Preview.
2. **`Preview`**:
   - Tabular preview of the first 10 rows with distinct headers and monospace cell values.
   - Total row & column counts, and horizontally scrollable container.
   - Action buttons: `"← Choose different file"` and `"Confirm & Start Loading"`.
   - **Empty state**: If clicked before uploading, displays `"No dataset uploaded yet"` with a `"Go to Upload"` button.
3. **`Loading`**:
   - Progress bar showing `rows_loaded / rows_total` and live text count.
   - `rows_failed` highlighted in warning red if greater than 0.
   - Displays completion confirmation before transitioning to Chat.
   - **Empty state**: If clicked before initiating a loading pipeline, displays `"No active loading pipeline"` with actions to upload or load sample.
4. **`Chat`**:
   - Grounded Q&A interface with natural language answers, `Grounded ✓` / `Not grounded ⚠` badges, collapsible Cypher & raw Neo4j JSON, thinking animation, and suggested prompt chips.
   - **Notice**: If visited before a dataset is loaded, shows a calm notice and locks the input until data is ingested.

## How to Run
Open [index.html](file:///Users/guruprasanna/csv%20data%20chatbot/index.html) in your browser or run:

```bash
# Option A: Zero-dependency mock API server
python3 server.py

# Option B: Built-in static file server
python3 -m http.server 3000
```
Then visit `http://localhost:3000`.

## Pre-Freeze Quality Gate & Grounding Verification

Run the unified adversarial test suite before committing and pushing:

```bash
python3 test_grounding.py
```

### Pre-Freeze Checklist (Must 100% Pass Before Commit & Push at 7:25)
- [x] **Category A: General Knowledge Traps** (5 tests: France capital, 2+2, Apple CEO, WW2 end, Tesla stock) $\to$ `grounded: false`, no leaked facts.
- [x] **Category B: Absent Entities & Missing Columns** (2 tests: CUST-9999 proven effort, credit_score missing column) $\to$ `grounded: false`.
- [x] **Category C: Partial Matches** (1 test: Delta spending + credit score) $\to$ answers real data for Delta ($625.00) + explicit honesty disclaimer for credit score.
- [x] **Category D: Leading & Loaded Questions** (2 tests: Causality inquiry, AWS riskiest premise) $\to$ `grounded: false`, rejects unverified premises.
- [x] **Category E: Real Sanity Checks** (5 tests: total rows, settled rows, Delta spending, category spending, risk >= 0.5) $\to$ `grounded: true`.
- [x] **100% Passing Gate**: `test_grounding.py` returns exit code `0`. Blocks with non-zero exit code on any regression.
- [!] **Trigger Rule**: Re-run `python3 test_grounding.py` immediately whenever translation logic (`translator.py`), LLM prompts (`llm_layer.py`), or grounding checks (`grounding.py`) change.

