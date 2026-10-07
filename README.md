# HTAP State Store: Phase 1 (Engine and Adapter Core)

Transactional write path for multi-agent claims processing, per the guide's plan (Weeks 1-3).

## Run it

```bash
docker compose up -d                 # Postgres 16 on :5432
pip install -r requirements.txt
pytest -v                            # 12 tests
```

To use another Postgres-compatible server (for example a Lakebase instance), set
`STATE_STORE_DSN`. Lakebase has not been tested here.

## What is in this phase

| Guide's module | Where |
|---|---|
| CAS ticket-claiming | `claim_ticket` / `update_state` in `state_store/postgres_adapter.py` |
| Snapshot isolation | Write path runs `REPEATABLE READ` (configurable) |
| Bitemporal dual-row schema | `claims` (current row) + `claim_history` (versioned rows, valid and tx time), filled by a trigger in `sql/001_schema.sql` |
| `StateStoreAdapter` interface | `state_store/base.py` |
| Audit path (as-of reads) | `get_history`, `get_as_of` |

## Design choices worth knowing

- `version` is both the CAS token and the fencing token.
- Leases: a claim has `lease_expires_at`. A slow agent whose lease expired cannot write, and another agent can take over. This reproduces the long-tail-inference failure mode your paper opens with.
- Outcomes are `OK`, `CONFLICT` (clean CAS miss), `ABORTED` (engine serialization failure), `NOT_FOUND`. Keeping `CONFLICT` and `ABORTED` separate gives you the abort-rate curve.
- `update_state(..., release=True)` clears ownership so the next agent (triage, adjuster, fraud) can claim.

## Measured on a local Postgres 16 (20 rounds of 100 agents racing for one claim)

| Isolation | OK | Conflict | Aborted |
|---|---|---|---|
| repeatable_read | 20 | 111 | 1869 |
| read_committed | 20 | 1980 | 0 |

Exactly one winner per round in both modes (no double-claims). Under snapshot isolation most losers are aborted by the engine instead of seeing a clean miss. That is a real contention cost worth reporting in Phase 2.

## Not in Phase 1

- Retroactive corrections (`tx_to` is reserved but never closed).
- Columnar storage for the audit agents. History is a normal Postgres table for now; the Lakebase/Delta sync belongs to Phase 4.
- Redis and DynamoDB adapters, and the load harness (Phase 2).
- KV-cache streaming to vLLM (Phase 4).
