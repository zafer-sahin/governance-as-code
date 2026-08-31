"""
governance/src/recon_operator/differ.py

Pure dictionary diff engine for the Reconciliation Operator (Plane 4).

Responsibility:
  Compare two plain dicts — "desired state" (from Plane 2 JSON outputs)
  vs "actual state" (built from Plane 3 telemetry events) — and produce
  a structured list of DiffEntry objects describing every divergence.

Design:
  - 100% pure functions. No I/O, no async, no side effects.
  - Recursive: handles nested dicts to arbitrary depth.
  - Returns structured DiffEntry value objects (frozen dataclasses).
  - Never raises on type mismatches — records them as MODIFIED entries.
  - The is_clean() predicate lets callers gate on drift with one call.

Pattern: Declarative State Matching (Drift Detection) — Plane 4 of the GaC architecture.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto, unique
from typing import Any


# ---------------------------------------------------------------------------
# Diff entry types
# ---------------------------------------------------------------------------

@unique
class DiffKind(Enum):
    """Classification of a single diff observation."""
    ADDED    = auto()   # key present in desired but absent in actual
    REMOVED  = auto()   # key present in actual but absent in desired
    MODIFIED = auto()   # key present in both, values differ
    TYPE_MISMATCH = auto()  # same key, incompatible types (e.g., dict vs str)


@dataclass(frozen=True, slots=True)
class DiffEntry:
    """
    A single divergence between desired and actual state.

    Fields
    ------
    kind        : DiffKind — what type of difference this is.
    path        : Dot-separated key path to the divergent field
                  (e.g., "resources.analytics.pii_table.constraint.tag").
    desired_val : Value in the desired-state dict (None if REMOVED).
    actual_val  : Value in the actual-state dict (None if ADDED).
    dimension   : Governance dimension — "ranger" | "atlas" | "model-registry".
    severity    : "LOW" | "MEDIUM" | "HIGH" — inferred from key criticality.
    """
    kind:        DiffKind
    path:        str
    desired_val: Any
    actual_val:  Any
    dimension:   str = "unknown"
    severity:    str = "MEDIUM"

    def __str__(self) -> str:
        return (
            f"[{self.kind.name:<13}] {self.path!r:60s} "
            f"desired={_truncate(self.desired_val)}  "
            f"actual={_truncate(self.actual_val)}  "
            f"dim={self.dimension}  sev={self.severity}"
        )


# ---------------------------------------------------------------------------
# Diff result aggregate
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class DiffResult:
    """
    Complete diff output between one desired/actual pair.

    Fields
    ------
    resource_id  : Human-readable name for the compared resource
                   (e.g., policy name, entity qualified_name).
    dimension    : Governance dimension — "ranger" | "atlas" | "model-registry".
    entries      : All detected DiffEntry objects.
    desired_hash : Lightweight hash of the desired dict (for event payloads).
    actual_hash  : Lightweight hash of the actual dict.
    """
    resource_id:  str
    dimension:    str
    entries:      tuple[DiffEntry, ...]
    desired_hash: str
    actual_hash:  str

    @property
    def has_drift(self) -> bool:
        return len(self.entries) > 0

    @property
    def max_severity(self) -> str:
        order = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
        if not self.entries:
            return "NONE"
        return max(self.entries, key=lambda e: order.get(e.severity, 0)).severity

    def report(self) -> str:
        if not self.has_drift:
            return f"[{self.resource_id}] ✓ No drift detected."
        lines = [
            f"[{self.resource_id}] ✗ DRIFT_DETECTED — {len(self.entries)} divergence(s) "
            f"(max_severity={self.max_severity})"
        ]
        for entry in self.entries:
            lines.append(f"  {entry}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def diff(
    desired: dict[str, Any],
    actual:  dict[str, Any],
    *,
    resource_id: str = "unknown",
    dimension:   str = "unknown",
    _path_prefix: str = "",
) -> DiffResult:
    """
    Compute a deep diff between two dictionaries.

    Parameters
    ----------
    desired:
        The desired-state dict loaded from Plane 2 JSON outputs (Git).
    actual:
        The actual-state dict built from Plane 3 telemetry events.
    resource_id:
        A label for the compared resource (policy name, qualified_name…).
    dimension:
        Governance dimension tag ("ranger" | "atlas" | "model-registry").

    Returns
    -------
    DiffResult
        Contains all DiffEntry objects. Check `.has_drift` to gate.
    """
    entries = _deep_diff(desired, actual, dimension=dimension, path=_path_prefix)
    return DiffResult(
        resource_id=resource_id,
        dimension=dimension,
        entries=tuple(entries),
        desired_hash=_dict_hash(desired),
        actual_hash=_dict_hash(actual),
    )


def is_clean(result: DiffResult) -> bool:
    """Return True if no drift was detected."""
    return not result.has_drift


# ---------------------------------------------------------------------------
# Internal recursive differ
# ---------------------------------------------------------------------------

# Keys whose drift is always HIGH severity
_HIGH_SEVERITY_KEYS: frozenset[str] = frozenset({
    "constraint", "tag", "pii", "masking", "mask_type", "dataMaskType",
    "enforcedConstraint", "effect", "isEnabled", "isAuditEnabled",
    "classifications", "superTypes", "git_sha", "gitSha",
})

# Keys whose drift is LOW severity (metadata/cosmetic)
_LOW_SEVERITY_KEYS: frozenset[str] = frozenset({
    "description", "version", "labels", "capturedAt", "writtenAt",
    "compiledAt", "updatedBy", "createdBy", "compiledBy",
})


def _deep_diff(
    desired: Any,
    actual:  Any,
    *,
    dimension: str,
    path: str,
) -> list[DiffEntry]:
    """Recursively diff two values at the given dot-path."""
    entries: list[DiffEntry] = []

    # Both are dicts — recurse key by key
    if isinstance(desired, dict) and isinstance(actual, dict):
        all_keys = desired.keys() | actual.keys()
        for key in sorted(all_keys):
            child_path = f"{path}.{key}" if path else key
            if key not in actual:
                entries.append(_make_entry(DiffKind.ADDED, child_path, desired[key], None, dimension))
            elif key not in desired:
                entries.append(_make_entry(DiffKind.REMOVED, child_path, None, actual[key], dimension))
            else:
                entries.extend(_deep_diff(desired[key], actual[key], dimension=dimension, path=child_path))
        return entries

    # Type mismatch — one is dict, other is scalar
    if isinstance(desired, dict) != isinstance(actual, dict):
        entries.append(_make_entry(DiffKind.TYPE_MISMATCH, path, desired, actual, dimension))
        return entries

    # Both are lists — compare element-by-element (order-sensitive)
    if isinstance(desired, list) and isinstance(actual, list):
        max_len = max(len(desired), len(actual))
        for i in range(max_len):
            child_path = f"{path}[{i}]"
            if i >= len(actual):
                entries.append(_make_entry(DiffKind.ADDED, child_path, desired[i], None, dimension))
            elif i >= len(desired):
                entries.append(_make_entry(DiffKind.REMOVED, child_path, None, actual[i], dimension))
            else:
                entries.extend(_deep_diff(desired[i], actual[i], dimension=dimension, path=child_path))
        return entries

    # Scalar comparison
    if desired != actual:
        entries.append(_make_entry(DiffKind.MODIFIED, path, desired, actual, dimension))
    return entries


def _make_entry(
    kind: DiffKind,
    path: str,
    desired_val: Any,
    actual_val:  Any,
    dimension: str,
) -> DiffEntry:
    """Construct a DiffEntry with automatically inferred severity."""
    leaf = path.split(".")[-1].strip("[]0123456789")
    if leaf in _HIGH_SEVERITY_KEYS:
        severity = "HIGH"
    elif leaf in _LOW_SEVERITY_KEYS:
        severity = "LOW"
    else:
        severity = "MEDIUM"
    return DiffEntry(
        kind=kind,
        path=path,
        desired_val=desired_val,
        actual_val=actual_val,
        dimension=dimension,
        severity=severity,
    )


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def _truncate(value: Any, max_len: int = 40) -> str:
    s = repr(value)
    return s if len(s) <= max_len else s[:max_len - 1] + "…"


def _dict_hash(d: dict[str, Any]) -> str:
    """Lightweight deterministic hash of a dict (for DiffResult metadata)."""
    import hashlib, json
    raw = json.dumps(d, sort_keys=True, default=str).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()[:16]
