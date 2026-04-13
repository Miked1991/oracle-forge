# DuckDB Schemas for DAB Datasets

## Analytics Cube Dataset

### Table: sales_fact

- sale_id (BIGINT, PK)
- customer_id (INTEGER)
- product_id (INTEGER)
- sale_date (DATE)
- amount (DECIMAL(10,2))
- quantity (INTEGER)

### Table: time_dimension

- date_key (DATE, PK)
- year (INTEGER)
- quarter (INTEGER)
- month (INTEGER)
- fiscal_year (INTEGER)
- fiscal_quarter (INTEGER)

---

## crmarenapro Dataset

### Table: churn_predictions

Used in J1 (churn risk score join). Join key: `customer_ref` (TEXT, format `"C{customer_id}"` — strip leading `"C"` to match PostgreSQL INT `customer_id`).

- id (BIGINT, PK)
- customer_ref (TEXT)  — `"C1001"`, `"C1002"`, etc.
- churn_score (FLOAT)  — 0.0 to 1.0; `> 0.7` = high churn risk
- prediction_date (DATE)
- model_version (TEXT)

**J1 join fix:**
```python
df_churn['cid_int'] = df_churn['customer_ref'].str.lstrip('C').astype(int)
merged = pg_customers.merge(df_churn, left_on='customer_id', right_on='cid_int')
```

### Table: loyalty

Used in M6 (SQLite `customers` TEXT → DuckDB `loyalty` INTEGER join). Join key: `cust_id` (INTEGER, first 5 digits of SQLite `customer_id` numeric part after stripping prefix).

- id (BIGINT, PK)
- cust_id (INTEGER)   — e.g. 98765 (first 5 digits of SQLite `customer_id` like `"ID-98765"`)
- loyalty_points (INTEGER)
- tier (TEXT)         — `"bronze"`, `"silver"`, `"gold"`, `"platinum"`
- joined_date (DATE)

**M6 join fix:**
```python
from utils.join_key_resolver import JoinKeyResolver
resolver = JoinKeyResolver()
sqlite_df['cust_id_norm'] = sqlite_df['customer_id'].apply(
    lambda k: int(resolver.resolve_chain(k, ['strip_prefix', 'first_5_chars']))
)
merged = sqlite_df.merge(loyalty_df, left_on='cust_id_norm', right_on='cust_id')
```

---

## GitHub Dataset

### Table: contributors

Used in M4 (GitHub contributor–dependency graph). Join key: `contributor_login` (TEXT) matched against SQLite `dependencies.contributor_login`.

- id (BIGINT, PK)
- repo_id (BIGINT, FK → repositories.id in SQLite)
- contributor_login (TEXT)  — GitHub username, e.g. `"octocat"`
- commits (INTEGER)
- additions (INTEGER)
- deletions (INTEGER)

### Table: repositories (DuckDB mirror)

Analytical copy of the SQLite `repositories` table for window-function aggregations. `repo_id` is the canonical join key to SQLite.

- repo_id (BIGINT, PK)
- name (TEXT)
- language (TEXT)
- stars (INTEGER)
- forks (INTEGER)
- created_at (DATE)

**M4 execution pattern:**
```python
# DuckDB: unique contributors per repo
duck_df = duckdb_conn.execute(
    "SELECT repo_id, COUNT(DISTINCT contributor_login) AS contributor_count FROM contributors GROUP BY repo_id"
).df()

# SQLite: repos that appear in dependency graph
sqlite_df = sqlite_conn.execute(
    "SELECT DISTINCT dependent_repo_id AS repo_id FROM dependencies"
).fetchdf()

# Python merge — never cross-engine SQL
result = duck_df[duck_df['repo_id'].isin(sqlite_df['repo_id'])]
```

---

## PANCANCER_ATLAS Dataset

### Table: gene_expression

Used in M5 (TP53 high-risk mutation + low gene expression). Join key: `patient_id` (TEXT, format `"TCGA-AB-1234"`) — must be resolved to PostgreSQL format via `resolve_tcga_id()`.

- id (BIGINT, PK)
- patient_id (TEXT)        — `"TCGA-AB-1234"` format
- gene_symbol (TEXT)       — e.g. `"TP53"`, `"BRCA1"`
- expression_value (FLOAT) — normalised RPKM; low = < 2.0 threshold
- sample_type (TEXT)       — `"tumor"` or `"normal"`
- platform (TEXT)

**M5 join fix:**
```python
from utils.join_key_resolver import JoinKeyResolver
resolver = JoinKeyResolver()

# DuckDB: low TP53 expression patients
expr_df = duckdb_conn.execute(
    "SELECT patient_id, expression_value FROM gene_expression "
    "WHERE gene_symbol = 'TP53' AND expression_value < 2.0"
).df()
expr_df['pg_patient_id'] = expr_df['patient_id'].apply(resolver.resolve_tcga_id)

# PostgreSQL: high-risk TP53 mutations
mut_df = pg_conn.fetch("SELECT patient_id FROM mutations WHERE gene = 'TP53' AND risk_level = 'high'")
pg_df = pd.DataFrame(mut_df, columns=['patient_id'])

# Python merge on resolved key
result = expr_df[expr_df['pg_patient_id'].isin(pg_df['patient_id'])]
```

---

## Yelp Dataset

### Table: business

**`business.stars` is a stale pre-computed aggregate** — see Rating Source Warning below.

- business_id (TEXT, PK)   — 22-char alphanumeric, direct match with MongoDB
- name (TEXT)
- city (TEXT)
- state (TEXT)
- stars (FLOAT)            — ⚠ STALE weekly batch aggregate; do NOT use for rating queries
- review_count (INTEGER)
- is_open (INTEGER)        — 1 = open, 0 = permanently closed; ALWAYS filter on this
- categories (TEXT)        — pipe-separated: `"Restaurants|Pizza|Italian"`; use CategoryMatcher

### Table: checkin

Used in M2 (businesses with > 100 check-ins). Join key: `business_id` (TEXT, direct match with MongoDB `reviews.business_id`).

- business_id (TEXT, FK → business.business_id)
- checkin_date (DATE)
- checkin_count (INTEGER)  — daily check-in count

## Yelp Dataset — Rating Source Warning

**`business.stars` is a stale pre-computed aggregate**, updated weekly by a batch job.

For any query asking for "average rating", "review score", or rating-based filters on Yelp data:

- **Always recompute from `MongoDB reviews.stars`** grouped by `business_id`.
- **Do NOT use `business.stars`** as if it were a live per-review average.

```python
# Correct: recompute from live reviews
pipeline = [
    {"$group": {"_id": "$business_id", "avg_rating": {"$avg": "$stars"}}}
]

# Wrong: stale weekly aggregate
# SELECT stars FROM business WHERE ...
```

System prompt guard: "If query mentions 'rating' or 'review score' on Yelp data, ALWAYS join MongoDB reviews — do NOT use `business.stars`."

---

## Important for DAB

DuckDB is used for analytical queries that aggregate across large datasets.
Optimize for: GROUP BY, window functions, time-series analysis.

**Fiscal Calendar Note:** Telecom fiscal Q3 = Oct-Dec (not Jul-Sep)
Check kb/domain/domain_terms/business_glossary.md for dataset-specific calendars.

## DAB-Specific Query Examples

**Declining repeat purchase rate by segment (Q3):**

```sql
SELECT
    customer_segment,
    fiscal_quarter,
    COUNT(DISTINCT customer_id) AS customers,
    SUM(amount) AS revenue,
    revenue / LAG(revenue) OVER (PARTITION BY customer_segment ORDER BY fiscal_quarter) - 1 AS growth_rate
FROM sales_fact
JOIN time_dimension ON sales_fact.sale_date = time_dimension.date_key
WHERE time_dimension.fiscal_quarter = 3
  AND time_dimension.fiscal_year = 2025
GROUP BY customer_segment, fiscal_quarter;
```

**Ticket volume correlation (after conductor merges PG + Mongo):**

```sql
SELECT
    t.customer_id,
    t.revenue,
    m.ticket_count,
    CORR(t.revenue, m.ticket_count) OVER () AS revenue_ticket_correlation
FROM pg_transactions t
JOIN mongo_tickets m ON t.customer_id = m.customer_id;
```

**Window function for rolling 90-day active customers:**

```sql
SELECT
    customer_id,
    MAX(sale_date) AS last_purchase,
    CASE WHEN MAX(sale_date) >= CURRENT_DATE - INTERVAL '90 days' THEN 'active' ELSE 'inactive' END AS status
FROM sales_fact
GROUP BY customer_id;
```

## Injection Test

Q: What is DuckDB used for in DAB?
A: Analytical queries that aggregate across large datasets
