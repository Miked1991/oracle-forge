# DuckDB Schemas for DAB Datasets

## Analytics Cube Dataset

**Table: sales_fact**

- sale_id (BIGINT, PK)
- customer_id (INTEGER)
- product_id (INTEGER)
- sale_date (DATE)
- amount (DECIMAL(10,2))
- quantity (INTEGER)

**Table: time_dimension**

- date_key (DATE, PK)
- year (INTEGER)
- quarter (INTEGER)
- month (INTEGER)
- fiscal_year (INTEGER)
- fiscal_quarter (INTEGER)

## Important for DAB

DuckDB is used for analytical queries that aggregate across large datasets.
Optimize for: GROUP BY, window functions, time-series analysis.

**Fiscal Calendar Note:** Telecom fiscal Q3 = Oct-Dec (not Jul-Sep)
Check kb/v2-domain/domain_terms/business_glossary.md for dataset-specific calendars.

## Injection Test

Q: What is DuckDB used for in DAB?
A: Analytical queries that aggregate across large datasets
