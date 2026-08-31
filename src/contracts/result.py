"""
governance/src/contracts/result.py

Result[S, F] — Algebraic Data Type for deterministic success/failure modelling.

Design:
  - Sealed hierarchy via __init_subclass__ guard (Python-idiomatic sealed trait).
  - Fully immutable: frozen dataclasses, no mutation surface.
  - Generic over Success type S and Failure type F.
  - Exhaustive pattern matching via .fold(), .map(), .flat_map(), .recover().
  - Never raises; errors are values.

Pattern: Algebraic Data Types (ADTs) — from the GaC design documents (Plane 1).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable, Final, Generic, TypeVar

S = TypeVar("S")
F = TypeVar("F")
S2 = TypeVar("S2")
F2 = TypeVar("F2")

# ---------------------------------------------------------------------------
# Sealed base — only Success and Failure may subclass Result
# ---------------------------------------------------------------------------

_RESULT_SEALED: Final[set[str]] = {"Success", "Failure"}


class Result(Generic[S, F]):
    """
    Abstract sealed sum type.

    Concrete subtypes: Success[S, F] | Failure[S, F]
    """

    def __init_subclass__(cls, **kwargs: object) -> None:
        if cls.__name__ not in _RESULT_SEALED:
            raise TypeError(
                f"Result is a sealed type. '{cls.__name__}' is not a permitted subtype. "
                f"Only {_RESULT_SEALED} are allowed."
            )
        super().__init_subclass__(**kwargs)

    # ------------------------------------------------------------------
    # Combinators
    # ------------------------------------------------------------------

    def is_success(self) -> bool:
        return isinstance(self, Success)

    def is_failure(self) -> bool:
        return isinstance(self, Failure)

    def fold(
        self,
        on_success: Callable[[S], S2],
        on_failure: Callable[[F], S2],
    ) -> S2:
        """
        Exhaustive eliminator. Callers MUST handle both branches.
        Equivalent to Scala's `fold`, Haskell's `either`.
        """
        if isinstance(self, Success):
            return on_success(self.value)
        if isinstance(self, Failure):
            return on_failure(self.error)
        raise AssertionError("Unreachable: sealed type violated")  # pragma: no cover

    def map(self, f: Callable[[S], S2]) -> "Result[S2, F]":
        """Transform the success value; propagate failure unchanged."""
        if isinstance(self, Success):
            return Success(f(self.value))
        return self  # type: ignore[return-value]

    def map_failure(self, f: Callable[[F], F2]) -> "Result[S, F2]":
        """Transform the failure value; propagate success unchanged."""
        if isinstance(self, Failure):
            return Failure(f(self.error))
        return self  # type: ignore[return-value]

    def flat_map(self, f: Callable[[S], "Result[S2, F]"]) -> "Result[S2, F]":
        """Monadic bind. Chain operations that may themselves fail."""
        if isinstance(self, Success):
            return f(self.value)
        return self  # type: ignore[return-value]

    def recover(self, f: Callable[[F], S]) -> S:
        """Extract value, applying recovery function on failure."""
        return self.fold(lambda v: v, f)

    def get_or_raise(self) -> S:
        """
        Unsafe extractor. Use only at integration boundaries where
        raising is acceptable (e.g., CLI entry points).
        """
        if isinstance(self, Success):
            return self.value
        if isinstance(self, Failure):
            raise ValueError(f"Result.get_or_raise called on Failure: {self.error}")
        raise AssertionError("Unreachable")  # pragma: no cover

    def __repr__(self) -> str:
        if isinstance(self, Success):
            return f"Success({self.value!r})"
        if isinstance(self, Failure):
            return f"Failure({self.error!r})"
        return "Result(?)"  # pragma: no cover


# ---------------------------------------------------------------------------
# Concrete variants — immutable via frozen=True
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Success(Result[S, F]):
    """
    The happy-path variant.

    value: the successfully computed result of type S.
    """

    value: S


@dataclass(frozen=True, slots=True)
class Failure(Result[S, F]):
    """
    The error variant.

    error: the structured failure descriptor of type F.
    """

    error: F


# ---------------------------------------------------------------------------
# Convenience constructors (mirrors Rust's Ok / Err)
# ---------------------------------------------------------------------------


def ok(value: S) -> Result[S, F]:
    """Construct a Success."""
    return Success(value)


def err(error: F) -> Result[S, F]:
    """Construct a Failure."""
    return Failure(error)
