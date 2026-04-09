# Join Key Format Reference

## Critical: Always resolve formats before joining across databases

### Yelp Dataset (Consistent - no resolution needed)

- All business_id: 22-character alphanumeric strings
- All user_id: 22-character alphanumeric strings
- all customer_id : integer
- Format consistent across PostgreSQL and MongoDB

### Retail Dataset (Requires resolution)

```markdown
| Database | Format | Example |
|----------|--------|---------|
| PostgreSQL (transactions) | Integer | 12345 |
| MongoDB (CRM) | CUST-prefixed string | "CUST-12345" |
| SQLite (cache) | Zero-padded | "0000012345" |
```

**Resolution Strategy:**

### Pattern 1: Integer to String

```sql
-- PostgreSQL side
SELECT CAST(customer_id AS TEXT) as customer_id_str
FROM customers
``
-- Remove prefix for join

SELECT REPLACE(customer_id, 'CUST-', '') as customer_id_num
FROM mongodb_customers

-- Extract numeric portion
SELECT REGEXP_REPLACE(customer_id, '[^0-9]', '', 'g') as customer_id_num
FROM customers
