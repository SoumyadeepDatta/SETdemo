import asyncio
import os

import pytest
import pytest_asyncio

from state_store import Outcome, PostgresAdapter

DSN = os.environ.get(
    "STATE_STORE_DSN", "postgresql://postgres:postgres@localhost:5432/state_store"
)


@pytest_asyncio.fixture
async def store():
    s = PostgresAdapter(DSN, max_size=120)
    await s.connect()
    await s.reset()
    yield s
    await s.close()


async def test_create_and_get(store):
    c = await store.create_claim("C1", {"amount": 1200})
    assert (c.status, c.owner, c.version) == ("NEW", None, 1)
    assert (await store.get_claim("C1")).payload == {"amount": 1200}
    # idempotent
    again = await store.create_claim("C1", {"amount": 999})
    assert again.payload == {"amount": 1200}
    assert await store.get_claim("missing") is None


async def test_claim_bumps_version_and_sets_owner(store):
    await store.create_claim("C1", {})
    r = await store.claim_ticket("C1", "agent-a", expected_version=1)
    assert r.ok
    assert (r.claim.owner, r.claim.version, r.claim.status) == ("agent-a", 2, "CLAIMED")


@pytest.mark.parametrize("isolation", ["repeatable_read", "read_committed"])
async def test_no_double_claim_under_race(isolation):
    """100 agents race for one ticket: exactly one may win."""
    s = PostgresAdapter(DSN, max_size=120, isolation=isolation)
    await s.connect()
    await s.reset()
    await s.create_claim("HOT", {})

    results = await asyncio.gather(
        *[s.claim_ticket("HOT", f"agent-{i}", expected_version=1) for i in range(100)]
    )
    winners = [r for r in results if r.outcome is Outcome.OK]
    losers = [r for r in results if r.outcome is not Outcome.OK]

    assert len(winners) == 1
    assert all(r.outcome in (Outcome.CONFLICT, Outcome.ABORTED) for r in losers)
    final = await s.get_claim("HOT")
    assert final.version == 2
    assert final.owner == winners[0].claim.owner
    if isolation == "read_committed":  # loser re-checks WHERE, so never aborts
        assert all(r.outcome is Outcome.CONFLICT for r in losers)
    await s.close()


async def test_stale_version_rejected(store):
    await store.create_claim("C1", {})
    await store.claim_ticket("C1", "a", 1)
    r = await store.update_state("C1", "a", expected_version=1, new_status="TRIAGED")
    assert r.outcome is Outcome.CONFLICT and r.current_version == 2


async def test_update_requires_ownership(store):
    await store.create_claim("C1", {})
    await store.claim_ticket("C1", "a", 1)
    r = await store.update_state("C1", "intruder", 2, "TRIAGED")
    assert r.outcome is Outcome.CONFLICT
    ok = await store.update_state("C1", "a", 2, "TRIAGED", {"risk": "low"})
    assert ok.ok and ok.claim.payload == {"risk": "low"} and ok.claim.version == 3


async def test_release_hands_off_to_next_agent(store):
    await store.create_claim("C1", {})
    await store.claim_ticket("C1", "triage", 1)
    r = await store.update_state("C1", "triage", 2, "TRIAGED", release=True)
    assert r.ok and r.claim.owner is None
    nxt = await store.claim_ticket("C1", "adjuster", r.claim.version)
    assert nxt.ok and nxt.claim.owner == "adjuster"


async def test_not_found(store):
    r = await store.claim_ticket("nope", "a", 1)
    assert r.outcome is Outcome.NOT_FOUND


async def test_lease_expiry_fences_slow_agent(store):
    """The paper's failure mode: a slow LLM call outlives its lease."""
    await store.create_claim("C1", {})
    slow = await store.claim_ticket("C1", "slow-agent", 1, lease_seconds=0.2)
    assert slow.ok

    # while the lease is live, nobody else can take it
    early = await store.claim_ticket("C1", "fast-agent", slow.claim.version)
    assert early.outcome is Outcome.CONFLICT

    await asyncio.sleep(0.35)  # lease expires during "inference"

    # the slow agent is now fenced out of writing...
    late = await store.update_state("C1", "slow-agent", slow.claim.version, "TRIAGED")
    assert late.outcome is Outcome.CONFLICT
    # ...and another agent can take over
    takeover = await store.claim_ticket("C1", "fast-agent", slow.claim.version)
    assert takeover.ok and takeover.claim.owner == "fast-agent"
    # the old agent still cannot write with its stale version
    stale = await store.update_state("C1", "slow-agent", slow.claim.version, "TRIAGED")
    assert not stale.ok


async def test_terminal_claims_cannot_be_reclaimed(store):
    await store.create_claim("C1", {})
    await store.claim_ticket("C1", "a", 1)
    done = await store.update_state("C1", "a", 2, "APPROVED", release=True)
    assert done.ok
    r = await store.claim_ticket("C1", "b", done.claim.version)
    assert r.outcome is Outcome.CONFLICT


async def test_history_and_bitemporal_as_of(store):
    await store.create_claim("C1", {"step": "created"})
    await asyncio.sleep(0.02)
    t_after_create = await store.db_now()
    await asyncio.sleep(0.02)

    await store.claim_ticket("C1", "a", 1)
    await asyncio.sleep(0.02)
    t_after_claim = await store.db_now()
    await asyncio.sleep(0.02)

    await store.update_state("C1", "a", 2, "FRAUD_REVIEW", {"flag": True})

    hist = await store.get_history("C1")
    assert [h["version"] for h in hist] == [1, 2, 3]
    assert [h["status"] for h in hist] == ["NEW", "CLAIMED", "FRAUD_REVIEW"]
    # contiguous, non-overlapping valid-time intervals; last one is open
    for prev, nxt in zip(hist, hist[1:]):
        assert prev["valid_to"] == nxt["valid_from"]
    assert (
        hist[-1]["valid_to"].isoformat().startswith("infinity")
        or hist[-1]["valid_to"].year > 9000
    )

    # point-in-time reads
    assert (await store.get_as_of("C1", t_after_create))["status"] == "NEW"
    mid = await store.get_as_of("C1", t_after_claim)
    assert (mid["status"], mid["owner"]) == ("CLAIMED", "a")
    now = await store.get_as_of("C1", await store.db_now())
    assert now["status"] == "FRAUD_REVIEW" and now["payload"]["flag"] is True
    # before the claim existed
    assert await store.get_as_of("C1", hist[0]["valid_from"].replace(year=2000)) is None


async def test_audit_scan_does_not_block_writers(store):
    """History reads use a different table, so a long audit scan never
    holds locks on the hot `claims` rows."""
    for i in range(50):
        await store.create_claim(f"C{i}", {})
    audit = asyncio.ensure_future(
        asyncio.gather(*[store.get_history(f"C{i}") for i in range(50)])
    )
    r = await asyncio.gather(*[store.claim_ticket(f"C{i}", "w", 1) for i in range(50)])
    await audit
    assert all(x.ok for x in r)
