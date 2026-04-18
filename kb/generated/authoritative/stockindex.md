# AUTHORITATIVE — Schema registry snapshot

**Trust tier:** `AUTHORITATIVE` — generated from `artifacts/schema_registry/*.json` (live introspection). This section overrides informal schema prose elsewhere.

- **dataset_id:** `stockindex`
- **schema_registry_version:** `1.0`
- **registry built_at_utc:** `2026-04-18T10:04:02.668256+00:00`
- **datasets_config:** `eval\datasets.json`

## Dataset summary

Registry for benchmark dataset `stockindex`: combined schema from all reachable engines. Engine overview: sqlite: index_info | duckdb: index_trade | postgresql: business, business_category, review, user

## Engines

### Engine `duckdb`

#### Tables

- **Table** `index_trade`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Index` — VARCHAR — nullable=True — is_primary_key=False
    - `Date` — VARCHAR — nullable=True — is_primary_key=False
    - `Open` — DOUBLE — nullable=True — is_primary_key=False
    - `High` — DOUBLE — nullable=True — is_primary_key=False
    - `Low` — DOUBLE — nullable=True — is_primary_key=False
    - `Close` — DOUBLE — nullable=True — is_primary_key=False
    - `Adj Close` — DOUBLE — nullable=True — is_primary_key=False
    - `CloseUSD` — DOUBLE — nullable=True — is_primary_key=False

### Engine `mongodb`

*Unavailable — skipped_reason: `mongodb_database_not_configured_in_eval_datasets_json`*

### Engine `postgresql`

#### Tables

- **Table** `business`
  - **primary_key:** `business_id`
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `business_id` — text — nullable=False — is_primary_key=True
    - `name` — text — nullable=True — is_primary_key=False
    - `description` — text — nullable=True — is_primary_key=False
    - `review_count` — integer — nullable=True — is_primary_key=False
    - `is_open` — integer — nullable=True — is_primary_key=False
    - `attributes` — text — nullable=True — is_primary_key=False
    - `hours` — text — nullable=True — is_primary_key=False
    - `state_code` — text — nullable=True — is_primary_key=False
    - `accepts_credit_cards` — boolean — nullable=True — is_primary_key=False
    - `has_wifi` — boolean — nullable=True — is_primary_key=False
    - `primary_categories` — text — nullable=True — is_primary_key=False

- **Table** `business_category`
  - **primary_key:** `business_id`, `category`
  - **foreign_keys:** 1
    - `['business_id']` → `business`(`business_id`)
  - **columns:**
    - `business_id` — text — nullable=False — is_primary_key=True
    - `category` — text — nullable=False — is_primary_key=True

- **Table** `review`
  - **primary_key:** `review_id`
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `review_id` — text — nullable=False — is_primary_key=True
    - `user_id` — text — nullable=True — is_primary_key=False
    - `business_id` — text — nullable=True — is_primary_key=False
    - `stars` — integer — nullable=True — is_primary_key=False
    - `date` — text — nullable=True — is_primary_key=False
    - `text` — text — nullable=True — is_primary_key=False

- **Table** `user`
  - **primary_key:** `user_id`
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `user_id` — text — nullable=False — is_primary_key=True
    - `name` — text — nullable=True — is_primary_key=False
    - `review_count` — integer — nullable=True — is_primary_key=False
    - `yelping_since` — text — nullable=True — is_primary_key=False
    - `useful` — integer — nullable=True — is_primary_key=False
    - `funny` — integer — nullable=True — is_primary_key=False
    - `cool` — integer — nullable=True — is_primary_key=False
    - `elite` — text — nullable=True — is_primary_key=False

### Engine `sqlite`

#### Tables

- **Table** `index_info`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Exchange` — TEXT — nullable=True — is_primary_key=False
    - `Currency` — TEXT — nullable=True — is_primary_key=False

---

**ADVISORY** documents (`kb/domain/**`, join prose, glossary) are hints only — they must not contradict this authoritative snapshot for identifiers.