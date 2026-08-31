"""
governance/src/local_proxy/__init__.py

Public API for the LocalProxy (Plane 1 — Shift-Left Developer Proxy).

Import surface:

    from local_proxy import generate_trino_execution_plan
"""

from local_proxy.trino_plan_stub import generate_trino_execution_plan

__all__ = ["generate_trino_execution_plan"]
