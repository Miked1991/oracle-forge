# Ill-Formatted Join Key Mappings Across DAB Databases

## Yelp Dataset

| Entity | PostgreSQL Format | MongoDB Format | Transformation |
|--------|------------------|----------------|----------------|
| business_id | "abc123def456" (TEXT) | "abc123def456" (STRING) | Direct match |
| user_id | "user_12345" (TEXT) | "USER-12345" (STRING) | user_id.replace('user_', 'USER-') |
| review_id | "xyz789abc123" (TEXT) | "xyz789abc123" (STRING) | Direct match |

## Telecom Dataset

| Entity | PostgreSQL Format | MongoDB Format | Transformation |
|--------|------------------|----------------|----------------|
| subscriber_id | 1234567 (INT) | "CUST-1234567" (STRING) | f"CUST-{subscriber_id}" |
| ticket_id | "TKT-12345678" (TEXT) | "TKT-12345678" (STRING) | Direct match |

## Healthcare Dataset

| Entity | PostgreSQL Format | MongoDB Format | Transformation |
|--------|------------------|----------------|----------------|
| patient_id | 987654321 (INT) | "PT-987654321" (STRING) | f"PT-{patient_id}" |
| provider_npi | 1234567890 (INT) | "NPI-1234567890" (STRING) | f"NPI-{provider_npi}" |

## Detection Logic

When join fails, call `resolve_join_key(value, source_table, target_table)` to detect and fix the mismatch:

1. Check if one side is INT, other is STRING with prefix
2. Extract numeric part: re.search(r'\d+', string_value)
3. Compare numeric values
4. Apply correct transformation based on table name

## Code Implementation

```python
def resolve_join_key(value, source_table, target_table):
    if source_table == 'subscribers' and 'mongo' in target_table:
        if isinstance(value, int):
            return f"CUST-{value}"
    if 'mongo' in source_table and target_table == 'subscribers':
        if isinstance(value, str) and value.startswith('CUST-'):
            return int(value.replace('CUST-', ''))
    return value
```

## Injection Test

Q: How do I join PostgreSQL subscriber_id to MongoDB?
A: Use resolve_join_key to apply the transformation f"CUST-{subscriber_id}" when joining PostgreSQL subscriber_id to MongoDB.
