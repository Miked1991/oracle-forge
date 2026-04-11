
---

## File: `kb/v2-domain/databases/sqlite_schemas.md`

```markdown
# SQLite Schemas for DAB Datasets

## Transaction Logs Dataset

**Table: transactions**
- transaction_id (INTEGER, PK)
- customer_id (INTEGER) - format: integer, no prefix
- product_id (INTEGER)
- quantity (INTEGER)
- unit_price (REAL)
- transaction_date (TEXT) - ISO format 'YYYY-MM-DD'
- store_location (TEXT)

**Table: products**
- product_id (INTEGER, PK)
- product_name (TEXT)
- category (TEXT)
- supplier_id (INTEGER)

## Inventory Dataset

**Table: inventory**
- inventory_id (INTEGER, PK)
- product_id (INTEGER)
- store_id (INTEGER)
- stock_level (INTEGER)
- last_restock_date (TEXT)

## Note for Joins

SQLite uses INTEGER IDs, same as PostgreSQL.
MongoDB requires transformation: f"CUST-{customer_id}"

## Injection Test
Q: What format are customer_ids in SQLite?
A: INTEGER, no prefix
