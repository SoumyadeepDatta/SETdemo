import asyncio
from state_store import PostgresAdapter

async def main():
    store = PostgresAdapter("postgresql://postgres:postgres@localhost:5432/state_store")
    await store.connect()
    await store.reset()                       # wipes all tickets
    await store.create_claim("C1", {"amount": 1200})

    a = await store.claim_ticket("C1", "triage-agent", expected_version=1)
    if a.claim is not None:
        print(a.outcome, a.claim.version, a.claim.owner)   # OK, 2, triage-agent
    else:
        pass

    b = await store.claim_ticket("C1", "adjuster", expected_version=1)
    print(b.outcome, b.current_version)                # CONFLICT, 2

    for row in await store.get_history("C1"):
        print(row["version"], row["status"], row["owner"])
    await store.close()

asyncio.run(main())