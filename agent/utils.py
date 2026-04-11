import json
import math
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple


NEGATIVE_INDICATORS = {
    "frustrated",
    "angry",
    "terrible",
    "awful",
    "worst",
    "broken",
    "not working",
    "failed",
    "error",
    "complaint",
    "unhappy",
    "disappointed",
    "useless",
    "waste",
    "horrible",
    "unacceptable",
    "furious",
    "annoyed",
    "upset",
}


def canonical_db_name(name: str) -> str:
    text = (name or "").strip().lower()
    if "post" in text:
        return "postgresql"
    if "mongo" in text:
        return "mongodb"
    if "duck" in text:
        return "duckdb"
    if "sqlite" in text:
        return "sqlite"
    return text


def safe_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    except TypeError:
        return str(value)


def result_summary(value: Any) -> str:
    if isinstance(value, dict):
        keys = list(value.keys())
        return f"dict(keys={keys[:6]})"
    if isinstance(value, list):
        return f"list(len={len(value)})"
    return str(value)[:160]


def extract_numeric_fragment(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value)
    match = re.search(r"\d+", text)
    return int(match.group(0)) if match else None


def normalize_join_key(
    value: Any,
    source_db: Optional[str] = None,
    target_db: Optional[str] = None,
    entity_hint: Optional[str] = None,
) -> str:
    source = canonical_db_name(source_db or "")
    target = canonical_db_name(target_db or "")
    entity = (entity_hint or "").lower()
    if isinstance(value, str):
        value = value.strip()
    numeric = extract_numeric_fragment(value)
    if numeric is None:
        return str(value).strip().lower()
    if target == "mongodb":
        if "patient" in entity:
            return f"PT-{numeric}"
        if "provider" in entity or "npi" in entity:
            return f"NPI-{numeric}"
        if "user" in entity:
            return f"USER-{numeric}"
        return f"CUST-{numeric}"
    if source == "mongodb" and target in {"postgresql", "sqlite", "duckdb"}:
        return str(numeric)
    return str(numeric)


def normalize_for_compare(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        trimmed = value.strip()
        numeric = extract_numeric_fragment(trimmed)
        if numeric is not None and ("-" in trimmed or "_" in trimmed):
            return str(numeric)
        return trimmed.lower()
    return str(value).strip().lower()


def normalize_records(records: Iterable[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in records:
        copy_row = dict(row)
        if key in copy_row:
            copy_row[f"__norm_{key}"] = normalize_for_compare(copy_row[key])
        normalized.append(copy_row)
    return normalized


def join_records(
    left_rows: List[Dict[str, Any]],
    right_rows: List[Dict[str, Any]],
    left_key: str,
    right_key: str,
) -> List[Dict[str, Any]]:
    if not left_rows or not right_rows:
        return []
    indexed: Dict[str, List[Dict[str, Any]]] = {}
    for row in right_rows:
        candidate = normalize_for_compare(row.get(right_key))
        indexed.setdefault(candidate, []).append(row)
    merged: List[Dict[str, Any]] = []
    for row in left_rows:
        candidate = normalize_for_compare(row.get(left_key))
        for partner in indexed.get(candidate, []):
            joined = dict(row)
            for key, value in partner.items():
                if key not in joined:
                    joined[key] = value
                else:
                    joined[f"right_{key}"] = value
            merged.append(joined)
    return merged


def detect_sentiment(text: Any) -> str:
    value = str(text or "").lower()
    if "not bad" in value:
        return "non-negative"
    if "not good" in value:
        return "negative"
    return "negative" if any(indicator in value for indicator in NEGATIVE_INDICATORS) else "non-negative"


def compute_metrics(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "row_count": len(records),
        "negative_sentiment_count": 0,
        "high_value_with_tickets": 0,
        "total_sales": 0.0,
    }
    for row in records:
        text = row.get("issue_description") or row.get("text") or row.get("clinical_notes")
        if text and detect_sentiment(text) == "negative":
            metrics["negative_sentiment_count"] += 1
        revenue = row.get("monthly_revenue")
        tickets = row.get("ticket_count", 0)
        if isinstance(revenue, (int, float)) and revenue > 100 and int(tickets or 0) > 0:
            metrics["high_value_with_tickets"] += 1
        total_sales = row.get("total_sales")
        amount = row.get("amount")
        if isinstance(total_sales, (int, float)):
            metrics["total_sales"] += float(total_sales)
        elif isinstance(amount, (int, float)):
            metrics["total_sales"] += float(amount)
    metrics["total_sales"] = round(metrics["total_sales"], 2)
    return metrics


def infer_join_key(rows: List[Dict[str, Any]]) -> Optional[str]:
    candidates = ["customer_id", "subscriber_id", "business_id", "user_id", "patient_id", "provider_npi"]
    if not rows:
        return None
    keys = set(rows[0].keys())
    for candidate in candidates:
        if candidate in keys:
            return candidate
    return None


def confidence_score(
    total_steps: int,
    successful_steps: int,
    retries: int,
    explicit_failure: bool,
    used_mock_mode: bool,
) -> float:
    if total_steps == 0:
        return 0.0
    step_ratio = successful_steps / total_steps
    retry_penalty = min(0.25, retries * 0.08)
    failure_penalty = 0.35 if explicit_failure else 0.0
    mock_penalty = 0.05 if used_mock_mode else 0.0
    score = step_ratio - retry_penalty - failure_penalty - mock_penalty
    return max(0.01, round(min(1.0, score), 3))


def wilson_interval(successes: int, total: int, z: float = 1.96) -> Tuple[float, float]:
    if total <= 0:
        return 0.0, 0.0
    phat = successes / total
    denominator = 1 + z * z / total
    center = phat + z * z / (2 * total)
    margin = z * math.sqrt((phat * (1 - phat) + z * z / (4 * total)) / total)
    return (center - margin) / denominator, (center + margin) / denominator


def classify_failure(error_text: str, payload: Optional[Dict[str, Any]] = None) -> str:
    text = (error_text or "").lower()
    sql = str((payload or {}).get("sql", "")).lower()
    if "syntax" in text or "dialect" in text or "sql" in text:
        return "sql_dialect_error"
    if "join" in text or "cust-" in text or "mismatch" in text:
        return "join_mismatch"
    if "column" in text or "table" in text or "schema" in text or "unknown field" in text:
        return "schema_mismatch"
    if "timeout" in text or "connection" in text:
        return "execution_error"
    if sql and " join " in sql and "no such column" in text:
        return "schema_mismatch"
    return "unknown_error"


def sanitize_error(error_text: str) -> str:
    text = (error_text or "").strip()
    if not text:
        return "Execution failed."
    lowered = text.lower()
    if "password" in lowered or "token" in lowered or "dsn" in lowered:
        return "Execution failed due to connection configuration."
    if len(text) > 220:
        return text[:217] + "..."
    return text
