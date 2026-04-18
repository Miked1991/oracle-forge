# AUTHORITATIVE — Schema registry snapshot

**Trust tier:** `AUTHORITATIVE` — generated from `artifacts/schema_registry/*.json` (live introspection). This section overrides informal schema prose elsewhere.

- **dataset_id:** `DEPS_DEV_V1`
- **schema_registry_version:** `1.0`
- **registry built_at_utc:** `2026-04-18T10:03:56.921403+00:00`
- **datasets_config:** `eval\datasets.json`

## Dataset summary

Registry for benchmark dataset `DEPS_DEV_V1`: combined schema from all reachable engines. Engine overview: sqlite: project_info, project_packageversion | duckdb: packageinfo | postgresql: business, business_category, review, user

## Engines

### Engine `duckdb`

#### Tables

- **Table** `packageinfo`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `System` — VARCHAR — nullable=True — is_primary_key=False
    - `Name` — VARCHAR — nullable=True — is_primary_key=False
    - `Version` — VARCHAR — nullable=True — is_primary_key=False
    - `Licenses` — VARCHAR — nullable=True — is_primary_key=False
    - `Links` — VARCHAR — nullable=True — is_primary_key=False
    - `Advisories` — VARCHAR — nullable=True — is_primary_key=False
    - `VersionInfo` — VARCHAR — nullable=True — is_primary_key=False
    - `Hashes` — VARCHAR — nullable=True — is_primary_key=False
    - `DependenciesProcessed` — BIGINT — nullable=True — is_primary_key=False
    - `DependencyError` — BIGINT — nullable=True — is_primary_key=False
    - `UpstreamPublishedAt` — DOUBLE — nullable=True — is_primary_key=False
    - `Registries` — VARCHAR — nullable=True — is_primary_key=False
    - `SLSAProvenance` — DOUBLE — nullable=True — is_primary_key=False
    - `UpstreamIdentifiers` — VARCHAR — nullable=True — is_primary_key=False
    - `Purl` — DOUBLE — nullable=True — is_primary_key=False

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

- **Table** `project_info`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Project_Information` — VARCHAR — nullable=True — is_primary_key=False
    - `Licenses` — VARCHAR — nullable=True — is_primary_key=False
    - `Description` — VARCHAR — nullable=True — is_primary_key=False
    - `Homepage` — VARCHAR — nullable=True — is_primary_key=False
    - `OSSFuzz` — DOUBLE — nullable=True — is_primary_key=False

- **Table** `project_packageversion`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `System` — VARCHAR — nullable=True — is_primary_key=False
    - `Name` — VARCHAR — nullable=True — is_primary_key=False
    - `Version` — VARCHAR — nullable=True — is_primary_key=False
    - `ProjectType` — VARCHAR — nullable=True — is_primary_key=False
    - `ProjectName` — VARCHAR — nullable=True — is_primary_key=False
    - `RelationProvenance` — VARCHAR — nullable=True — is_primary_key=False
    - `RelationType` — VARCHAR — nullable=True — is_primary_key=False

---

**ADVISORY** documents (`kb/domain/**`, join prose, glossary) are hints only — they must not contradict this authoritative snapshot for identifiers.