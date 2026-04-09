# Database Schemas for DAB Benchmark

## Yelp Dataset (PostgreSQL)

### Table: yelp_business

```markdown
| Column | Type | Description |
|--------|------|-------------|
| business_id | VARCHAR(22) | Primary key, unique identifier |
| name | VARCHAR(255) | Business name |
| address | VARCHAR(255) | Street address |
| city | VARCHAR(100) | City |
| state | VARCHAR(2) | Two-letter state code |
| postal_code | VARCHAR(10) | ZIP code |
| latitude | FLOAT | GPS coordinate |
| longitude | FLOAT | GPS coordinate |
| stars | FLOAT | Average rating (1-5) |
| review_count | INTEGER | Number of reviews |
| is_open | INTEGER | 1 = open, 0 = closed |
| attributes | JSON | Business attributes (parking, wifi, etc.) |
| categories | VARCHAR(500) | Comma-separated category list |
| hours | JSON | Operating hours per day |
```

### Table: yelp_review

```markdown
| Column | Type | Description |
|--------|------|-------------|
| review_id | VARCHAR(22) | Primary key |
| user_id | VARCHAR(22) | References yelp_user |
| business_id | VARCHAR(22) | References yelp_business |
| stars | INTEGER | Rating (1-5) |
| useful | INTEGER | Number of useful votes |
| funny | INTEGER | Number of funny votes |
| cool | INTEGER | Number of cool votes |
| text | TEXT | Full review text (unstructured) |
| date | DATE | Review date |
```

### Table: yelp_user

```markdown
| Column | Type | Description |
|--------|------|-------------|
| user_id | VARCHAR(22) | Primary key |
| name | VARCHAR(255) | User name |
| review_count | INTEGER | Total reviews written |
| yelping_since | DATE | Account creation date |
| useful | INTEGER | Total useful votes received |
| funny | INTEGER | Total funny votes received |
| cool | INTEGER | Total cool votes received |
| fans | INTEGER | Number of fans |
| average_stars | FLOAT | Average rating given |
| compliment_* | INTEGER | Various compliment counts |
```

## MongoDB Collections (Yelp)

### Collection: reviews

- Same structure as yelp_review but with nested JSON
- Contains full text with no schema enforcement
- Key format: review_id as string, business_id as string

## Critical: Key Format Differences

- PostgreSQL yelp_business.business_id: VARCHAR(22), e.g., "abc123def456"
- MongoDB reviews.business_id: Same string format (consistent for Yelp)
- For other datasets: Customer IDs differ (integers vs prefixed strings)
