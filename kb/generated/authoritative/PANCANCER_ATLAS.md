# AUTHORITATIVE — Schema registry snapshot

**Trust tier:** `AUTHORITATIVE` — generated from `artifacts/schema_registry/*.json` (live introspection). This section overrides informal schema prose elsewhere.

- **dataset_id:** `PANCANCER_ATLAS`
- **schema_registry_version:** `1.0`
- **registry built_at_utc:** `2026-04-18T10:03:59.188855+00:00`
- **datasets_config:** `eval\datasets.json`

## Dataset summary

Registry for benchmark dataset `PANCANCER_ATLAS`: combined schema from all reachable engines. Engine overview: duckdb: Mutation_Data, RNASeq_Expression | postgresql: business, business_category, review, user

## Engines

### Engine `duckdb`

#### Tables

- **Table** `Mutation_Data`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `ParticipantBarcode` — VARCHAR — nullable=True — is_primary_key=False
    - `Tumor_SampleBarcode` — VARCHAR — nullable=True — is_primary_key=False
    - `Tumor_AliquotBarcode` — VARCHAR — nullable=True — is_primary_key=False
    - `Normal_SampleBarcode` — VARCHAR — nullable=True — is_primary_key=False
    - `Normal_AliquotBarcode` — VARCHAR — nullable=True — is_primary_key=False
    - `Normal_SampleTypeLetterCode` — VARCHAR — nullable=True — is_primary_key=False
    - `Hugo_Symbol` — VARCHAR — nullable=True — is_primary_key=False
    - `HGVSp_Short` — VARCHAR — nullable=True — is_primary_key=False
    - `Variant_Classification` — VARCHAR — nullable=True — is_primary_key=False
    - `HGVSc` — VARCHAR — nullable=True — is_primary_key=False
    - `CENTERS` — VARCHAR — nullable=True — is_primary_key=False
    - `FILTER` — VARCHAR — nullable=True — is_primary_key=False

- **Table** `RNASeq_Expression`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `ParticipantBarcode` — VARCHAR — nullable=True — is_primary_key=False
    - `SampleBarcode` — VARCHAR — nullable=True — is_primary_key=False
    - `AliquotBarcode` — VARCHAR — nullable=True — is_primary_key=False
    - `SampleTypeLetterCode` — VARCHAR — nullable=True — is_primary_key=False
    - `SampleType` — VARCHAR — nullable=True — is_primary_key=False
    - `Symbol` — VARCHAR — nullable=True — is_primary_key=False
    - `Entrez` — BIGINT — nullable=True — is_primary_key=False
    - `normalized_count` — DOUBLE — nullable=True — is_primary_key=False

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

*Unavailable — skipped_reason: `sqlite_path_missing_or_unreadable`*

---

**ADVISORY** documents (`kb/domain/**`, join prose, glossary) are hints only — they must not contradict this authoritative snapshot for identifiers.