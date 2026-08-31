"""
governance/src/recon_operator/__init__.py

Public API for the Reconciliation Operator (Plane 4 — Control Loop).

Import surface:

    from recon_operator import ControlLoop, StateStore
    from recon_operator import diff, DiffResult, DiffEntry, DiffKind
"""

from recon_operator.control_loop import ControlLoop
from recon_operator.differ import DiffEntry, DiffKind, DiffResult, diff, is_clean
from recon_operator.state_store import StateStore

__all__ = [
    "ControlLoop",
    "StateStore",
    "diff",
    "is_clean",
    "DiffResult",
    "DiffEntry",
    "DiffKind",
]
