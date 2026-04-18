# AUTHORITATIVE — Schema registry snapshot

**Trust tier:** `AUTHORITATIVE` — generated from `artifacts/schema_registry/*.json` (live introspection). This section overrides informal schema prose elsewhere.

- **dataset_id:** `yelp`
- **schema_registry_version:** `1.0`
- **registry built_at_utc:** `2026-04-18T10:04:16.553676+00:00`
- **datasets_config:** `eval\datasets.json`

## Dataset summary

Registry for benchmark dataset `yelp`: combined schema from all reachable engines. Engine overview: duckdb: review, tip, user | postgresql: business, business_category, review, user | mongodb: business, checkin

## Engines

### Engine `duckdb`

#### Tables

- **Table** `review`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `review_id` — VARCHAR — nullable=True — is_primary_key=False
    - `user_id` — VARCHAR — nullable=True — is_primary_key=False
    - `business_ref` — VARCHAR — nullable=True — is_primary_key=False
    - `rating` — BIGINT — nullable=True — is_primary_key=False
    - `useful` — BIGINT — nullable=True — is_primary_key=False
    - `funny` — BIGINT — nullable=True — is_primary_key=False
    - `cool` — BIGINT — nullable=True — is_primary_key=False
    - `text` — VARCHAR — nullable=True — is_primary_key=False
    - `date` — VARCHAR — nullable=True — is_primary_key=False

- **Table** `tip`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `user_id` — VARCHAR — nullable=True — is_primary_key=False
    - `business_ref` — VARCHAR — nullable=True — is_primary_key=False
    - `text` — VARCHAR — nullable=True — is_primary_key=False
    - `date` — VARCHAR — nullable=True — is_primary_key=False
    - `compliment_count` — BIGINT — nullable=True — is_primary_key=False

- **Table** `user`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `user_id` — VARCHAR — nullable=True — is_primary_key=False
    - `name` — VARCHAR — nullable=True — is_primary_key=False
    - `review_count` — BIGINT — nullable=True — is_primary_key=False
    - `yelping_since` — VARCHAR — nullable=True — is_primary_key=False
    - `useful` — BIGINT — nullable=True — is_primary_key=False
    - `funny` — BIGINT — nullable=True — is_primary_key=False
    - `cool` — BIGINT — nullable=True — is_primary_key=False
    - `elite` — VARCHAR — nullable=True — is_primary_key=False

### Engine `mongodb`

#### MongoDB collections

- **Collection** `business`
  - **fields:**
    - `_id` — ObjectId
    - `attributes` — dict
    - `business_id` — str
    - `description` — str
    - `hours` — dict
    - `is_open` — int
    - `name` — str
    - `review_count` — int

- **Collection** `checkin`
  - **fields:**
    - `_id` — ObjectId
    - `business_id` — str
    - `date` — str

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

*Unavailable — skipped_reason: `sqlite_path_missing_or_unreadable`*

---

**ADVISORY** documents (`kb/domain/**`, join prose, glossary) are hints only — they must not contradict this authoritative snapshot for identifiers.