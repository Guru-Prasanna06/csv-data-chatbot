# Live Demo Script & Report Results: Grounding & Honesty Layer

> **Dataset Used for Demo:** `Q3 Corporate Expenses & Wire Transactions.csv`  
> **Discovered Schema Columns:** `amount`, `category`, `city`, `merchant`, `risk_score`, `status`, `tx_id`  
> **Total Records:** 8 transactions ($16,626.45 USD total volume)

---

## 1. Curated Demo Script & Results Table (REPORT.md Section 9.4)

| # | Demo Category | User Question | Expected Grounded | Expected Answer / Result Summary | One-Line Reasoning for Judges |
|:---:|:---|:---|:---:|:---|:---|
| **1** | **Easy Win** | `"Total spending by category"` | `true` | Led by `Wire Transfer` at $9,500.00 USD, followed by `Equipment` at $3,150.00 USD across 5 categories. | Demonstrates fuzzy column synonym resolution (`spending` $\to$ `amount`), group ranking, and zero invented numbers. |
| **2** | **Easy Win** | `"Count of rows where status is SETTLED"` | `true` | *"Found 5 rows where `status` is 'SETTLED'."* | Tests parameterized equality filtering and exact deterministic record counting. |
| **3** | **Easy Win** | `"What is the total spending for merchant Delta Air Lines"` | `true` | *"For `Delta Air Lines`, the total `amount` is $625.00 USD across 1 transaction."* | Tests entity-specific aggregation isolating a named merchant with exact dollar accounting. |
| **4** | **Edge Case** | `"Find rows where risk_score >= 0.89"` | `true` | 1 record returned: TX-104 (Unknown Offshore FX, risk: 0.89). | **Exact boundary test:** Filters at the maximum range edge; correctly extracts the single boundary outlier. |
| **5** | **Edge Case** | `"Show me transactions above $9000"` | `true` | 1 record returned: TX-104 (Wire Transfer, $9,500.00 USD). | **Extreme threshold test:** Verifies currency stripping (`$9000` $\to$ `9000`) and upper-tail anomaly retrieval. |
| **6** | **Honesty Test** | `"What is the total spending for customer CUST-9999"` | `false` | *"I ran an aggregation query for entity 'CUST-9999', but found no matching records in the uploaded data."* | **Proven Effort Test:** Runs real Cypher, finds 0 records, and exposes the empty array rather than hallucinating an answer. |
| **7** | **Honesty Test** | `"Filter rows where employee_performance_score > 90"` | `false` | *"I don't have data to answer that question. This dataset contains columns: `amount`, `category`... Try asking about..."* | **Schema Integrity Test:** Rejects nonexistent column and transparently suggests valid questions based on real columns. |
| **8** | **Honesty Test** | `"What is Apple's stock price and today's weather forecast?"` | `false` | *"I don't have data about weather & meteorological. This dataset contains columns: `amount`, `category`..."* | **Zero-Query Pre-Check:** Catches out-of-domain concepts instantly without wasting a database query. |

---

## 2. Live Demo Presenter Talk-Track (Step-by-Step Walkthrough)

### Step 1: The "Easy Wins" (Showing High-Speed Accuracy & Grounding)
1. **Type Question 1:** `"Total spending by category"`
   * **What to tell judges:** *"Notice the CSV column is actually named `amount`, but our translation layer maps 'spending' to 'amount' dynamically. The natural language answer quotes the exact $9,500.00 figure from the graph database."*
   * **Show in UI:** Point to the **[Grounded ✓]** badge and expand the details drawer to show the parameterized Cypher query.
2. **Type Question 2:** `"Count of rows where status is SETTLED"`
   * **What to tell judges:** *"The system parameterizes the query without string concatenation, preventing Cypher injection, and returns the exact count of 5 records."*

---

### Step 2: The "Edge Cases" (Range Boundaries & Outliers)
3. **Type Question 4:** `"Find rows where risk_score >= 0.89"`
   * **What to tell judges:** *"Here we test the exact numerical boundary of our fraud risk scores. Instead of a vague answer, it executes a `>=` filter and returns the single flagged wire transfer at 0.89."*
4. **Type Question 5:** `"Show me transactions above $9000"`
   * **What to tell judges:** *"The parser strips the dollar symbol, applies an upper-bound filter, and isolates the single highest transaction in the dataset."*

---

### Step 3: The "Honesty Tests" (The Key Differentiator)
5. **Type Question 6:** `"What is the total spending for customer CUST-9999"`
   * **What to tell judges:** *"Most AI chatbots will either guess a random number or say 'I don't know'. Watch what our system does: it awards a **[Not Grounded ⚠]** badge, but if you expand the details, you can see the REAL Cypher query that ran. We prove to the user that we actually checked the graph, found 0 records, and refused to fabricate data."*
6. **Type Question 7:** `"Filter rows where employee_performance_score > 90"`
   * **What to tell judges:** *"The user asked about an employee performance column that doesn't exist in a financial transaction CSV. Rather than forcing a bad query, our honesty layer lists the exact 7 columns available and suggests questions that will work."*
7. **Type Question 8:** `"What is Apple's stock price and today's weather forecast?"`
   * **What to tell judges:** *"External out-of-domain concepts are caught before Cypher generation. We protect the database from wasteful queries while keeping the response helpful and transparent."*

---

### Step 4: Terminal Proof for Technical Judges
Switch to the terminal running `python3 server.py 3000` to show live real-time audit logs:

```text
⚡ [11:51:02] [✓ GROUNDED]   | Q: "Total spending by category"                        | 5 row(s) | 0.32ms
   ↳ Cypher: MATCH (d:Dataset)-[:HAS_ROW]->(r:Row) WHERE ($dataset_id IS NULL OR d.id = $dataset_i...
⚡ [11:51:14] [✓ GROUNDED]   | Q: "Find rows where risk_score >= 0.89"                | 1 row(s) | 0.41ms
   ↳ Cypher: MATCH (d:Dataset)-[:HAS_ROW]->(r:Row) WHERE ($dataset_id IS NULL OR d.id = $dataset_i...
⚡ [11:51:28] [✗ UNGROUNDED] | Q: "What is the total spending for customer CUST-9999" | 0 row(s) | 0.30ms
   ↳ Cypher: MATCH (d:Dataset)-[:HAS_ROW]->(r:Row) WHERE ($dataset_id IS NULL OR d.id = $dataset_i...
⚡ [11:51:40] [✗ UNGROUNDED] | Q: "What is Apple's stock price and today's weather?"  | 0 row(s) | 0.00ms
```
* **Judge takeaway:** Proves that every request is checked dynamically against the Neo4j graph in sub-millisecond execution times.
