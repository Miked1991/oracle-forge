# AUTHORITATIVE — Schema registry snapshot

**Trust tier:** `AUTHORITATIVE` — generated from `artifacts/schema_registry/*.json` (live introspection). This section overrides informal schema prose elsewhere.

- **dataset_id:** `crmarenapro`
- **schema_registry_version:** `1.0`
- **registry built_at_utc:** `2026-04-18T10:04:02.226691+00:00`
- **datasets_config:** `eval\datasets.json`

## Dataset summary

Registry for benchmark dataset `crmarenapro`: combined schema from all reachable engines. Engine overview: sqlite: Account, Contact, User | duckdb: Order, OrderItem, Pricebook2, PricebookEntry, Product2, ProductCategory, ProductCategoryProduct | postgresql: business, business_category, review, user

## Engines

### Engine `duckdb`

#### Tables

- **Table** `Order`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Id` — VARCHAR — nullable=True — is_primary_key=False
    - `AccountId` — VARCHAR — nullable=True — is_primary_key=False
    - `Status` — VARCHAR — nullable=True — is_primary_key=False
    - `EffectiveDate` — VARCHAR — nullable=True — is_primary_key=False
    - `Pricebook2Id` — VARCHAR — nullable=True — is_primary_key=False
    - `OwnerId` — VARCHAR — nullable=True — is_primary_key=False

- **Table** `OrderItem`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Id` — VARCHAR — nullable=True — is_primary_key=False
    - `OrderId` — VARCHAR — nullable=True — is_primary_key=False
    - `Product2Id` — VARCHAR — nullable=True — is_primary_key=False
    - `Quantity` — VARCHAR — nullable=True — is_primary_key=False
    - `UnitPrice` — VARCHAR — nullable=True — is_primary_key=False
    - `PriceBookEntryId` — VARCHAR — nullable=True — is_primary_key=False

- **Table** `Pricebook2`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Id` — VARCHAR — nullable=True — is_primary_key=False
    - `Name` — VARCHAR — nullable=True — is_primary_key=False
    - `Description` — VARCHAR — nullable=True — is_primary_key=False
    - `IsActive` — BIGINT — nullable=True — is_primary_key=False
    - `ValidFrom` — VARCHAR — nullable=True — is_primary_key=False
    - `ValidTo` — VARCHAR — nullable=True — is_primary_key=False

- **Table** `PricebookEntry`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Id` — VARCHAR — nullable=True — is_primary_key=False
    - `Pricebook2Id` — VARCHAR — nullable=True — is_primary_key=False
    - `Product2Id` — VARCHAR — nullable=True — is_primary_key=False
    - `UnitPrice` — VARCHAR — nullable=True — is_primary_key=False

- **Table** `Product2`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Id` — VARCHAR — nullable=True — is_primary_key=False
    - `Name` — VARCHAR — nullable=True — is_primary_key=False
    - `Description` — VARCHAR — nullable=True — is_primary_key=False
    - `IsActive` — BIGINT — nullable=True — is_primary_key=False
    - `External_ID__c` — VARCHAR — nullable=True — is_primary_key=False

- **Table** `ProductCategory`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Id` — VARCHAR — nullable=True — is_primary_key=False
    - `Name` — VARCHAR — nullable=True — is_primary_key=False
    - `CatalogId` — VARCHAR — nullable=True — is_primary_key=False

- **Table** `ProductCategoryProduct`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Id` — VARCHAR — nullable=True — is_primary_key=False
    - `ProductCategoryId` — VARCHAR — nullable=True — is_primary_key=False
    - `ProductId` — VARCHAR — nullable=True — is_primary_key=False

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

- **Table** `Account`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Id` — TEXT — nullable=True — is_primary_key=False
    - `Name` — TEXT — nullable=True — is_primary_key=False
    - `Phone` — TEXT — nullable=True — is_primary_key=False
    - `Industry` — TEXT — nullable=True — is_primary_key=False
    - `Description` — TEXT — nullable=True — is_primary_key=False
    - `NumberOfEmployees` — REAL — nullable=True — is_primary_key=False
    - `ShippingState` — TEXT — nullable=True — is_primary_key=False

- **Table** `Contact`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Id` — TEXT — nullable=True — is_primary_key=False
    - `FirstName` — TEXT — nullable=True — is_primary_key=False
    - `LastName` — TEXT — nullable=True — is_primary_key=False
    - `Email` — TEXT — nullable=True — is_primary_key=False
    - `AccountId` — TEXT — nullable=True — is_primary_key=False

- **Table** `User`
  - **primary_key:** *(none in metadata)*
  - **foreign_keys:** *(none in metadata)*
  - **columns:**
    - `Id` — TEXT — nullable=True — is_primary_key=False
    - `FirstName` — TEXT — nullable=True — is_primary_key=False
    - `LastName` — TEXT — nullable=True — is_primary_key=False
    - `Email` — TEXT — nullable=True — is_primary_key=False
    - `Phone` — TEXT — nullable=True — is_primary_key=False
    - `Username` — TEXT — nullable=True — is_primary_key=False
    - `Alias` — TEXT — nullable=True — is_primary_key=False
    - `LanguageLocaleKey` — TEXT — nullable=True — is_primary_key=False
    - `EmailEncodingKey` — TEXT — nullable=True — is_primary_key=False
    - `TimeZoneSidKey` — TEXT — nullable=True — is_primary_key=False
    - `LocaleSidKey` — TEXT — nullable=True — is_primary_key=False

---

**ADVISORY** documents (`kb/domain/**`, join prose, glossary) are hints only — they must not contradict this authoritative snapshot for identifiers.