# Ill-Formatted Join Key Mappings Across DAB Databases

## Yelp Dataset

| Entity | PostgreSQL Format | MongoDB Format | Transformation |
| ------ | ---------------- | -------------- | -------------- |
| business_id | "abc123def456" (TEXT) | "abc123def456" (STRING) | Direct match |
| user_id | "user_12345" (TEXT) | "USER-12345" (STRING) | See idempotent transform in Code Implementation |
| review_id | "xyz789abc123" (TEXT) | "xyz789abc123" (STRING) | Direct match |

## Telecom Dataset

| Entity | PostgreSQL Format | MongoDB Format | Transformation |
| ------ | ---------------- | -------------- | -------------- |
| subscriber_id | 1234567 (INT) | "CUST-1234567" (STRING) | f"CUST-{subscriber_id}" |
| ticket_id | "TKT-12345678" (TEXT) | "TKT-12345678" (STRING) | Direct match |

## Healthcare Dataset

| Entity | PostgreSQL Format | MongoDB Format | Transformation |
| ------ | ---------------- | -------------- | -------------- |
| patient_id | 987654321 (INT) | "PT-987654321" (STRING) | f"PT-{patient_id}" |
| provider_npi | 1234567890 (INT) | "NPI-1234567890" (STRING) | f"NPI-{provider_npi}" |

## Detection Logic

When a join fails, use `JoinKeyResolver` from `utils/join_key_resolver.py` to detect and fix the mismatch:

1. Check if one side is INT, other is STRING with prefix
2. Extract numeric part: `re.sub(r'\D', '', string_value)`
3. Compare numeric values
4. Apply correct transformation based on table name

## Code Implementation

```python
from utils.join_key_resolver import JoinKeyResolver

resolver = JoinKeyResolver()

# PostgreSQL INT → MongoDB STRING (Telecom: subscriber_id → "CUST-{id}")
def pg_to_mongo_telecom(pg_int_id: int) -> str:
    return f"CUST-{pg_int_id}"

# PostgreSQL INT → MongoDB STRING (Healthcare: patient_id → "PT-{id}")
def pg_to_mongo_healthcare(pg_int_id: int) -> str:
    return f"PT-{pg_int_id}"

# Generic cross-DB key resolution (auto-detects normalization needed)
pg_key, mongo_key = resolver.resolve_cross_db_join(
    left_key=subscriber_id,
    right_key=mongo_ref,
    left_db_type='postgresql',
    right_db_type='mongodb'
)

def transform_yelp_user_id(user_id: str) -> str:
    """Idempotent: safe to call even if already in USER- format."""
    if user_id.startswith('USER-'):
        return user_id  # already transformed — do not double-transform
    return user_id.replace('user_', 'USER-')
```

## PANCANCER_ATLAS Dataset (M5)

| Entity | DuckDB Format | PostgreSQL Format | Transformation |
| ------ | ------------- | ----------------- | -------------- |
| patient_id | "TCGA-AB-1234" (STRING) | "ab1234" (UUID-style, no prefix/dashes) | Strip "TCGA-" prefix, remove all dashes, lowercase |

Resolution: `JoinKeyResolver.resolve_tcga_id("TCGA-AB-1234")` → `"ab1234"`

```python
# DuckDB gene_expression.patient_id:  "TCGA-AB-1234"
# PostgreSQL mutations.patient_id:    "ab1234"
# Fix: strip prefix and dashes, lowercase
resolved = resolver.resolve_tcga_id(duckdb_id)  # → "ab1234"
```

## Yelp Dataset — business_id (confirmed direct match)

`business_id` between DuckDB and MongoDB is a direct 22-character alphanumeric string. No normalization needed.

```text
DuckDB business.business_id:    "abc123def456xyz789ab12"  (TEXT)
MongoDB reviews.business_id:    "abc123def456xyz789ab12"  (STRING)
Transformation: direct equality
```

## Injection Test

Q: How do I join PostgreSQL subscriber_id to MongoDB?
A: Use JoinKeyResolver().resolve_cross_db_join() from utils/join_key_resolver.py. For Telecom apply f"CUST-{subscriber_id}" to convert the PostgreSQL INT to the MongoDB STRING format.
