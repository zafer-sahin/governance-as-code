"""
governance/src/gac_compiler/__init__.py

Public API for the GaC State Compiler (Plane 2 — Hexagonal Core).

Import surface:

    from gac_compiler import compile_plan, write_compilation_result, CompilationResult, WriteReport
"""

from gac_compiler.core_domain import CompilationResult, compile_plan
from gac_compiler.outbound_port import WriteReport, write_compilation_result

__all__ = [
    "compile_plan",
    "CompilationResult",
    "write_compilation_result",
    "WriteReport",
]
