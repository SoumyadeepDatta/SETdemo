"""PostgreSQL / Lakebase implementation of StateStoreAdapter.

Lakebase speaks the Postgres wire protocol, so pointing `dsn` at a Lakebase
instance should be the only change needed (not yet verified against a live
Lakebase endpoint).

Concurrency model
-----------------
Every write is a single-statement compare-and-swap:

    UPDATE claims SET ... version = version + 1
     WHERE claim_id = $1 AND version = $expected AND <ownership/lease guard>

The write path runs under REPEATABLE READ, which is snapshot isolation in
Postgres. Two outcomes are possible for the loser of a race:

  * The winner committed before the loser's snapshot  -> 0 rows updated
    -> CONFLICT (a clean CAS miss).
  * The winner committed after the loser's snapshot    -> Postgres raises a
    serialization failure -> ABORTED.

Keeping these apart lets the benchmark report CAS misses and engine-level aborts
separately (the "abort rate" curve in the paper). Set isolation="read_committed"
to compare: there the loser re-evaluates the WHERE clause and always sees a CONFLICT.
"""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import asyncpg

from .base import CasResult, Claim, Outcome, StateStoreAdapter

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "sql" / "001_schema.sql"

_RETURNING = "claim_id, status, owner, version, payload, lease_expires_at"
_TERMINAL = "('APPROVED','REJECTED','CLOSED')"

_CLAIM_SQL = f"""
UPDATE claims
   SET owner            = $2,
       status           = 'CLAIMED',
       version          = version + 1,
       lease_expires_at = clock_timestamp() + make_interval(secs => $4::double precision),
       last_actor       = $2,
       updated_at       = clock_timestamp()
 WHERE claim_id = $1
   AND version  = $3
   AND status NOT IN {_TERMINAL}
   AND (owner IS NULL OR lease_expires_at IS NULL OR lease_expires_at < clock_timestamp())
RETURNING {_RETURNING}
"""

_UPDATE_SQL = f"""
UPDATE claims
   SET status           = $4,
       payload          = payload || $5::jsonb,
       version          = version + 1,
       owner            = CASE WHEN $6::boolean THEN NULL ELSE owner END,
       lease_expires_at = CASE WHEN $6::boolean THEN NULL ELSE lease_expires_at END,
       last_actor       = $2,
       updated_at       = clock_timestamp()
 WHERE claim_id = $1
   AND version  = $3
   AND owner    = $2
   AND lease_expires_at > clock_timestamp()
RETURNING {_RETURNING}
"""


def _to_claim(row: asyncpg.Record) -> Claim:
    return Claim(
        claim_id=row["claim_id"],
        status=row["status"],
        owner=row["owner"],
        version=row["version"],
        payload=row["payload"],
        lease_expires_at=row["lease_expires_at"],
    )


async def _init_connection(conn: asyncpg.Connection) -> None:
    # Return jsonb as Python dicts and accept dicts as input.
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


class PostgresAdapter(StateStoreAdapter):
    def __init__(
        self,
        dsn: str,
        min_size: int = 1,
        max_size: int = 20,
        isolation: str = "repeatable_read",
    ) -> None:
        if isolation not in ("repeatable_read", "read_committed", "serializable"):
            raise ValueError(f"unsupported isolation level: {isolation}")
        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._isolation = isolation
        self._pool: Optional[asyncpg.Pool] = None

    # lifecycle ------------------------------------------------------------
    async def connect(self) -> None:
        self._pool = await asyncpg.create_pool(
            self._dsn,
            min_size=self._min_size,
            max_size=self._max_size,
            init=_init_connection,
        )
        await self.apply_schema()

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    async def apply_schema(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA_PATH.read_text())

    async def reset(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute("TRUNCATE claims, claim_history RESTART IDENTITY")

    async def db_now(self) -> datetime:
        """The database clock. Use this (not the Python clock) for as-of queries."""
        async with self._pool.acquire() as conn:
            return await conn.fetchval("SELECT clock_timestamp()")

    # writes ---------------------------------------------------------------
    async def create_claim(
        self, claim_id: str, payload: dict[str, Any], actor: str = "system"
    ) -> Claim:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                INSERT INTO claims (claim_id, payload, last_actor)
                VALUES ($1, $2::jsonb, $3)
                ON CONFLICT (claim_id) DO NOTHING
                RETURNING {_RETURNING}
                """,
                claim_id, payload, actor,
            )
            if row is None:  # already existed
                row = await conn.fetchrow(
                    f"SELECT {_RETURNING} FROM claims WHERE claim_id = $1", claim_id
                )
            return _to_claim(row)

    async def claim_ticket(
        self,
        claim_id: str,
        agent_id: str,
        expected_version: int,
        lease_seconds: float = 30.0,
    ) -> CasResult:
        return await self._cas(
            _CLAIM_SQL, claim_id, agent_id, expected_version, float(lease_seconds)
        )

    async def update_state(
        self,
        claim_id: str,
        agent_id: str,
        expected_version: int,
        new_status: str,
        payload_patch: Optional[dict[str, Any]] = None,
        release: bool = False,
    ) -> CasResult:
        """`release=True` clears owner and lease so the next agent in the
        pipeline (triage -> adjuster -> fraud) can claim the ticket."""
        return await self._cas(
            _UPDATE_SQL, claim_id, agent_id, expected_version,
            new_status, payload_patch or {}, release,
        )

    async def _cas(self, sql: str, claim_id: str, *args: Any) -> CasResult:
        # args[0] is agent_id, args[1] is expected_version (both SQL variants)
        async with self._pool.acquire() as conn:
            try:
                async with conn.transaction(isolation=self._isolation):
                    row = await conn.fetchrow(sql, claim_id, *args)
                    if row is not None:
                        return CasResult(Outcome.OK, claim=_to_claim(row))
                    current = await conn.fetchval(
                        "SELECT version FROM claims WHERE claim_id = $1", claim_id
                    )
            except (asyncpg.SerializationError, asyncpg.DeadlockDetectedError):
                return CasResult(Outcome.ABORTED)
        if current is None:
            return CasResult(Outcome.NOT_FOUND)
        return CasResult(Outcome.CONFLICT, current_version=current)

    # reads ----------------------------------------------------------------
    async def get_claim(self, claim_id: str) -> Optional[Claim]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f"SELECT {_RETURNING} FROM claims WHERE claim_id = $1", claim_id
            )
        return _to_claim(row) if row else None

    async def list_by_status(self, status: str, limit: int = 100) -> list[Claim]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT {_RETURNING} FROM claims WHERE status = $1 "
                "ORDER BY claim_id LIMIT $2",
                status, limit,
            )
        return [_to_claim(r) for r in rows]

    # audit path -----------------------------------------------------------
    async def get_history(self, claim_id: str) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT version, status, owner, payload, actor, "
                "       valid_from, valid_to, tx_from, tx_to "
                "FROM claim_history WHERE claim_id = $1 "
                "ORDER BY version, tx_from",
                claim_id,
            )
        return [dict(r) for r in rows]

    async def get_as_of(
        self,
        claim_id: str,
        valid_at: datetime,
        recorded_at: Optional[datetime] = None,
    ) -> Optional[dict[str, Any]]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT version, status, owner, payload, actor, "
                "       valid_from, valid_to, tx_from, tx_to "
                "FROM claim_history "
                "WHERE claim_id = $1 "
                "  AND valid_from <= $2 AND $2 < valid_to "
                "  AND tx_from <= COALESCE($3::timestamptz, clock_timestamp()) "
                "  AND COALESCE($3::timestamptz, clock_timestamp()) < tx_to "
                "ORDER BY version DESC LIMIT 1",
                claim_id, valid_at, recorded_at,
            )
        return dict(row) if row else None
