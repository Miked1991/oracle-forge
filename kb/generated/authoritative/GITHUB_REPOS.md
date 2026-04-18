# AUTHORITATIVE — Schema registry snapshot

**Trust tier:** `AUTHORITATIVE` — generated from `artifacts/schema_registry/*.json` (live introspection). This section overrides informal schema prose elsewhere.

- **dataset_id:** `GITHUB_REPOS`
- **schema_registry_version:** `1.0`
- **registry built_at_utc:** `2026-04-18T10:03:59.058834+00:00`
- **datasets_config:** `eval\datasets.json`

## Dataset summary

Registry for benchmark dataset `GITHUB_REPOS`: combined schema from all reachable engines. Engine overview: sqlite: languages, licenses, repos | duckdb: commits, contents, files | postgresql: business, business_category, review, user

## Engines

### Engine `duckdb`

#### Tables

- **Table** `commits`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `commit` — VARCHAR — nullable=True — is_primary_key=False
    - `tree` — VARCHAR — nullable=True — is_primary_key=False
    - `parent` — VARCHAR — nullable=True — is_primary_key=False
    - `author` — VARCHAR — nullable=True — is_primary_key=False
    - `committer` — VARCHAR — nullable=True — is_primary_key=False
    - `subject` — VARCHAR — nullable=True — is_primary_key=False
    - `message` — VARCHAR — nullable=True — is_primary_key=False
    - `trailer` — VARCHAR — nullable=True — is_primary_key=False
    - `difference` — VARCHAR — nullable=True — is_primary_key=False
    - `difference_truncated` — DOUBLE — nullable=True — is_primary_key=False
    - `repo_name` — VARCHAR — nullable=True — is_primary_key=False
    - `encoding` — VARCHAR — nullable=True — is_primary_key=False

- **Table** `contents`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `id` — VARCHAR — nullable=True — is_primary_key=False
    - `content` — VARCHAR — nullable=True — is_primary_key=False
    - `sample_repo_name` — VARCHAR — nullable=True — is_primary_key=False
    - `sample_ref` — VARCHAR — nullable=True — is_primary_key=False
    - `sample_path` — VARCHAR — nullable=True — is_primary_key=False
    - `sample_symlink_target` — VARCHAR — nullable=True — is_primary_key=False
    - `repo_data_description` — VARCHAR — nullable=True — is_primary_key=False

- **Table** `files`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `repo_name` — VARCHAR — nullable=True — is_primary_key=False
    - `ref` — VARCHAR — nullable=True — is_primary_key=False
    - `path` — VARCHAR — nullable=True — is_primary_key=False
    - `mode` — BIGINT — nullable=True — is_primary_key=False
    - `id` — VARCHAR — nullable=True — is_primary_key=False
    - `symlink_target` — VARCHAR — nullable=True — is_primary_key=False

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

- **Table** `languages`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `repo_name` — TEXT — nullable=True — is_primary_key=False
    - `language_description` — TEXT — nullable=True — is_primary_key=False

- **Table** `licenses`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `repo_name` — TEXT — nullable=True — is_primary_key=False
    - `license` — TEXT — nullable=True — is_primary_key=False

- **Table** `repos`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `repo_name` — TEXT — nullable=True — is_primary_key=False
    - `watch_count` — INTEGER — nullable=True — is_primary_key=False

---

**ADVISORY** documents (`kb/domain/**`, join prose, glossary) are hints only — they must not contradict this authoritative snapshot for identifiers.