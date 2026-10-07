-- Phase 1 schema: transactional write path + bitemporal dual-row history.
--
--   claims         : ONE current row per claim. The CAS target. `version` is the
--                    compare-and-swap token and also the fencing token.
--   claim_history  : one row per version, with TWO time axes
--                    (valid_* = business time, tx_* = system/recording time).
--                    Written automatically by a trigger, so any client that
--                    writes `claims` gets history for free.

CREATE TABLE IF NOT EXISTS claims (
    claim_id         text        PRIMARY KEY,
    status           text        NOT NULL DEFAULT 'NEW'
                     CHECK (status IN ('NEW','CLAIMED','TRIAGED','ADJUSTING',
                                       'FRAUD_REVIEW','APPROVED','REJECTED','CLOSED')),
    owner            text,
    version          bigint      NOT NULL DEFAULT 1,
    payload          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    lease_expires_at timestamptz,
    last_actor       text        NOT NULL DEFAULT 'system',
    updated_at       timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS claims_status_idx ON claims (status);

CREATE TABLE IF NOT EXISTS claim_history (
    history_id  bigserial   PRIMARY KEY,
    claim_id    text        NOT NULL REFERENCES claims (claim_id) ON DELETE CASCADE,
    version     bigint      NOT NULL,
    status      text        NOT NULL,
    owner       text,
    payload     jsonb       NOT NULL,
    actor       text        NOT NULL,
    valid_from  timestamptz NOT NULL,
    valid_to    timestamptz NOT NULL DEFAULT 'infinity',
    tx_from     timestamptz NOT NULL,
    tx_to       timestamptz NOT NULL DEFAULT 'infinity',
    UNIQUE (claim_id, version, tx_from)
);

CREATE INDEX IF NOT EXISTS claim_history_asof_idx
    ON claim_history (claim_id, valid_from, valid_to);

-- On every INSERT/UPDATE of `claims`: close the open history row and append the
-- new version. clock_timestamp() is read while the row lock is held, so the
-- timestamps are monotonic per claim even when transactions overlap.
-- tx_to stays 'infinity' for ordinary progress; it is reserved for retroactive
-- corrections (not implemented in Phase 1).
CREATE OR REPLACE FUNCTION claims_record_history() RETURNS trigger AS $$
DECLARE
    ts timestamptz := clock_timestamp();
BEGIN
    UPDATE claim_history
       SET valid_to = ts
     WHERE claim_id = NEW.claim_id
       AND valid_to = 'infinity';

    INSERT INTO claim_history
        (claim_id, version, status, owner, payload, actor, valid_from, tx_from)
    VALUES
        (NEW.claim_id, NEW.version, NEW.status, NEW.owner, NEW.payload,
         NEW.last_actor, ts, ts);

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS claims_history_trg ON claims;
CREATE TRIGGER claims_history_trg
    AFTER INSERT OR UPDATE ON claims
    FOR EACH ROW EXECUTE FUNCTION claims_record_history();
