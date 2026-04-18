"""Generate AUTHORITATIVE markdown KB from `artifacts/schema_registry/<dataset>.json`."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from utils.schema_registry.builder import build_schema_registry, default_registry_path
from utils.schema_registry.kb_log import log_kb_generation_event


def safe_dataset_filename(dataset_id: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in dataset_id.strip())


def authoritative_kb_relative_path(dataset_id: str) -> str:
    return f"generated/authoritative/{safe_dataset_filename(dataset_id)}.md"


def authoritative_kb_file_path(dataset_id: str, repo_root: Path) -> Path:
    return repo_root / "kb" / "generated" / "authoritative" / f"{safe_dataset_filename(dataset_id)}.md"


def load_registry_json(repo_root: Path, dataset_id: str) -> Dict[str, Any]:
    path = default_registry_path(dataset_id, repo_root)
    if not path.is_file():
        raise FileNotFoundError(f"Schema registry not found: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def render_authoritative_markdown(registry: Dict[str, Any]) -> str:
    """Render fixed-layout markdown; content is derived only from registry JSON."""
    lines: list[str] = []
    did = str(registry.get("dataset_id") or "").strip()
    ver = str(registry.get("schema_registry_version") or "")
    built = str(registry.get("built_at_utc") or "")
    lines.append("# AUTHORITATIVE — Schema registry snapshot")
    lines.append("")
    lines.append(
        "**Trust tier:** `AUTHORITATIVE` — generated from `artifacts/schema_registry/*.json` "
        "(live introspection). This section overrides informal schema prose elsewhere."
    )
    lines.append("")
    lines.append(f"- **dataset_id:** `{did}`")
    lines.append(f"- **schema_registry_version:** `{ver}`")
    lines.append(f"- **registry built_at_utc:** `{built}`")
    src = (registry.get("sources") or {}).get("datasets_config", "")
    if src:
        lines.append(f"- **datasets_config:** `{src}`")
    if registry.get("dataset_intent_summary"):
        lines.append("")
        lines.append("## Dataset summary")
        lines.append("")
        lines.append(str(registry["dataset_intent_summary"]))
    lines.append("")
    lines.append("## Engines")
    lines.append("")

    engines = registry.get("engines") or {}
    for eng_name in sorted(engines.keys()):
        eng = engines[eng_name]
        if not isinstance(eng, dict):
            continue
        lines.append(f"### Engine `{eng_name}`")
        lines.append("")
        if not eng.get("available"):
            sr = eng.get("skipped_reason") or "unknown"
            lines.append(f"*Unavailable — skipped_reason: `{sr}`*")
            lines.append("")
            continue

        tables = eng.get("tables") or []
        if tables:
            lines.append("#### Tables")
            lines.append("")
        for t in tables:
            if not isinstance(t, dict):
                continue
            tname = str(t.get("name") or "")
            lines.append(f"- **Table** `{tname}`")
            pk = t.get("primary_key") or []
            fks = t.get("foreign_keys") or []
            if pk:
                lines.append(f"  - **primary_key:** {', '.join(f'`{c}`' for c in pk)}")
            else:
                lines.append("  - **primary_key:** *(none in metadata)*")
            if fks:
                lines.append(f"  - **foreign_keys:** {len(fks)}")
                for fk in fks[:20]:
                    if not isinstance(fk, dict):
                        continue
                    cols = fk.get("columns") or []
                    rt = fk.get("referenced_table") or ""
                    rc = fk.get("referenced_columns") or []
                    lines.append(
                        f"    - `{cols}` → `{rt}`({', '.join(f'`{x}`' for x in rc)})"
                    )
                if len(fks) > 20:
                    lines.append(f"    - *(…{len(fks) - 20} more)*")
            else:
                lines.append("  - **foreign_keys:** *(none in metadata)*")
            cols = t.get("columns") or []
            if cols:
                lines.append("  - **columns:**")
                for c in cols:
                    if not isinstance(c, dict):
                        continue
                    cn = str(c.get("name") or "")
                    dt = str(c.get("data_type") or "")
                    nul = c.get("nullable")
                    ipk = c.get("is_primary_key")
                    lines.append(f"    - `{cn}` — {dt} — nullable={nul} — is_primary_key={ipk}")
            lines.append("")

        colls = eng.get("collections") or []
        if colls:
            lines.append("#### MongoDB collections")
            lines.append("")
        for c in colls:
            if not isinstance(c, dict):
                continue
            cname = str(c.get("name") or "")
            lines.append(f"- **Collection** `{cname}`")
            fields = c.get("fields") or c.get("columns") or []
            if isinstance(fields, list) and fields:
                lines.append("  - **fields:**")
                for f in fields[:200]:
                    if isinstance(f, dict):
                        fn = str(f.get("name") or f.get("field") or "")
                        ft = str(f.get("data_type") or f.get("type") or "")
                        lines.append(f"    - `{fn}` — {ft}")
                    else:
                        lines.append(f"    - {f}")
                if len(fields) > 200:
                    lines.append(f"  - *(…{len(fields) - 200} more fields)*")
            lines.append("")

    vj = registry.get("verified_joins")
    if isinstance(vj, list) and vj:
        lines.append("## Verified joins (from join metadata)")
        lines.append("")
        lines.append("```json")
        lines.append(json.dumps(vj, ensure_ascii=False, indent=2))
        lines.append("```")
        lines.append("")

    lines.append("---")
    lines.append("")
    lines.append(
        "**ADVISORY** documents (`kb/domain/**`, join prose, glossary) are hints only — "
        "they must not contradict this authoritative snapshot for identifiers."
    )
    return "\n".join(lines)


def write_authoritative_kb(
    dataset_id: str,
    repo_root: Path,
    *,
    registry: Optional[Dict[str, Any]] = None,
    log: bool = True,
    question: str = "",
) -> Tuple[Path, Dict[str, Any]]:
    """
    Write `kb/generated/authoritative/<dataset>.md` from registry JSON.

    Returns (written_path, summary dict).
    """
    root = repo_root
    t0 = time.perf_counter()
    out_path = authoritative_kb_file_path(dataset_id, root)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    reg_in = default_registry_path(dataset_id, root)
    err: Optional[str] = None
    status = "error"
    try:
        reg = registry if registry is not None else load_registry_json(root, dataset_id)
        md = render_authoritative_markdown(reg)
        out_path.write_text(md, encoding="utf-8")
        status = "ok"
    except Exception as exc:
        err = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        duration_ms = int((time.perf_counter() - t0) * 1000)
        rel_out = str(out_path.relative_to(root)) if out_path.is_file() else str(out_path)
        summary: Dict[str, Any] = {
            "dataset_id": dataset_id.strip(),
            "output_path": rel_out,
            "status": status,
            "error": err,
            "duration_ms": duration_ms,
        }
        if log:
            log_kb_generation_event(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "dataset_id": dataset_id.strip(),
                    "question": question or "(kb_generation)",
                    "phase": "kb_generation",
                    "input_artifact_refs": [str(reg_in.relative_to(root)) if reg_in.is_file() else str(reg_in)],
                    "output_artifact_refs": [rel_out],
                    "status": summary["status"],
                    "error": err,
                    "duration_ms": duration_ms,
                    "attempt_number": 1,
                },
                repo_root=root,
            )

    return out_path, summary


def generate_from_live_introspection(
    dataset_id: str,
    repo_root: Path,
    *,
    log_registry: bool = True,
    log_kb: bool = True,
) -> Tuple[Path, Path]:
    """Build registry JSON then authoritative KB (convenience for CI)."""
    _reg, reg_path = build_schema_registry(
        dataset_id,
        repo_root=repo_root,
        log=log_registry,
        persist=True,
    )
    kb_path, _ = write_authoritative_kb(dataset_id, repo_root, registry=None, log=log_kb)
    return reg_path, kb_path
