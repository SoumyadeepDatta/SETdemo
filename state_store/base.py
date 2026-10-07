"""Backend-agnostic interface for the agent state store.

Phase 2 adds RedisAdapter (WATCH/MULTI) and DynamoDBAdapter (conditional writes)
behind this same interface, so the benchmark harness never touches a backend
directly.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class Outcome(str, Enum):
    OK = "ok"              # write applied
    CONFLICT = "conflict"  # CAS lost: version/owner/lease did not match
    ABORTED = "aborted"    # backend aborted the txn (e.g. serialization failure)
    NOT_FOUND = "not_found"


@dataclass(frozen=True)
class Claim:
    claim_id: str
    status: str
    owner: Optional[str]
    version: int
    payload: dict[str, Any] = field(default_factory=dict)
    lease_expires_at: Optional[datetime] = None


@dataclass(frozen=True)
class CasResult:
    outcome: Outcome
    claim: Optional[Claim] = None          # set when outcome == OK
    current_version: Optional[int] = None  # set on CONFLICT, so callers can retry

    @property
    def ok(self) -> bool:
        return self.outcome is Outcome.OK


class StateStoreAdapter(ABC):
    """Contract every backend must satisfy.

    Semantics every backend must preserve (the benchmark measures how well they do):
      * claim_ticket / update_state are compare-and-swap on `version`.
      * A successful write bumps `version` by exactly 1.
      * update_state additionally requires the caller to be the current owner
        and the lease to still be valid (fencing).
    """

    # lifecycle ------------------------------------------------------------
    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def reset(self) -> None:
        """Delete all state. Used between benchmark runs."""

    # writes ---------------------------------------------------------------
    @abstractmethod
    async def create_claim(
        self, claim_id: str, payload: dict[str, Any], actor: str = "system"
    ) -> Claim:
        """Idempotent: returns the existing claim if claim_id already exists."""

    @abstractmethod
    async def claim_ticket(
        self,
        claim_id: str,
        agent_id: str,
        expected_version: int,
        lease_seconds: float = 30.0,
    ) -> CasResult:
        """Atomically take ownership iff version matches and the claim is
        unowned or its lease has expired."""

    @abstractmethod
    async def update_state(
        self,
        claim_id: str,
        agent_id: str,
        expected_version: int,
        new_status: str,
        payload_patch: Optional[dict[str, Any]] = None,
        release: bool = False,
    ) -> CasResult:
        """Move a claim to `new_status` iff version matches, agent_id is the
        owner and the lease is still valid. release=True also clears the
        owner so the next agent in the pipeline can claim it."""

    # reads ----------------------------------------------------------------
    @abstractmethod
    async def get_claim(self, claim_id: str) -> Optional[Claim]: ...

    @abstractmethod
    async def list_by_status(self, status: str, limit: int = 100) -> list[Claim]: ...

    # audit path -----------------------------------------------------------
    @abstractmethod
    async def get_history(self, claim_id: str) -> list[dict[str, Any]]:
        """All versions of a claim, oldest first."""

    @abstractmethod
    async def get_as_of(
        self,
        claim_id: str,
        valid_at: datetime,
        recorded_at: Optional[datetime] = None,
    ) -> Optional[dict[str, Any]]:
        """Bitemporal point-in-time read: what did the store say (as of
        recorded_at) about the claim's state at business time valid_at?"""
