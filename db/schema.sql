-- ============================================================================
-- flink_consumer — sink schema
--
-- Source of truth for all three sink tables. Applied once against an empty
-- Postgres data volume (mounted into docker-entrypoint-initdb.d in a later
-- pass). Naming convention for IDs (set by db/seed_roster.sql):
--   district code: 3-letter prefix, e.g. ADJ (Adjumani)
--   volunteer_id:  <district-code>-V<01-05>            e.g. ADJ-V01
--   group_id:      <volunteer_id>-G<1-2>                e.g. ADJ-V01-G1
--   participant_id:<group_id>-P<01-12>                  e.g. ADJ-V01-G1-P01
--   cycle_id:      CYCLE-01 (single demo cycle)
-- These are natural, human-readable keys deliberately — they show up as-is
-- in Kafka message keys and log lines, which matters for a recording-driven
-- demo (see CLAUDE.md "Demo & presentation").
-- ============================================================================

-- ----------------------------------------------------------------------------
-- group_roster
-- Static seed data, NOT stream-derived. Loaded once by db/seed_roster.sql and
-- never written to by the Flink pipeline. Represents the fixed participant
-- roster as set at group creation (week 1) — one row per participant per
-- group per cycle. Roster membership does not change mid-cycle; per-session
-- attendance (who actually showed up on a given week) is a separate concern
-- tracked in session_events.attendance / session_state.attendance, not here.
-- ----------------------------------------------------------------------------
CREATE TABLE group_roster (
    district TEXT NOT NULL
        CHECK (district IN ('Adjumani', 'Masaka', 'Mayuge', 'Moroto', 'Mbarara')),
    volunteer_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    cycle_id TEXT NOT NULL,
    participant_id TEXT NOT NULL,
    PRIMARY KEY (group_id, cycle_id, participant_id)
);

CREATE INDEX idx_group_roster_volunteer ON group_roster (volunteer_id);
CREATE INDEX idx_group_roster_district ON group_roster (district);

COMMENT ON TABLE group_roster IS
    'Static seed data (loaded once, not stream-derived). Fixed roster as of week 1 for each group/cycle. Not written to by the Flink pipeline.';
COMMENT ON COLUMN group_roster.participant_id IS
    'Roster membership only — whether this participant attended a specific week lives in session_events/session_state.attendance, not here.';

-- ----------------------------------------------------------------------------
-- session_events
-- Append-only, stream-derived. One row per submission received on the
-- session_submitted Kafka topic — every version of every submission gets its
-- own row, forming the full audit trail ("who submitted what, when"). Never
-- updated or deleted by the pipeline.
--
-- Versioning: there is no is_correction flag from the client. The Flink job
-- keys on the natural key (group_id, volunteer_id, week_number), tracks the
-- max version seen so far in Flink keyed state, and assigns max+1 (or 1 if
-- unseen) purely in-stream — no DB round-trip to compute it. `version` here
-- simply records what Flink assigned.
-- ----------------------------------------------------------------------------
CREATE TABLE session_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    group_id TEXT NOT NULL,
    volunteer_id TEXT NOT NULL,
    week_number SMALLINT NOT NULL
        CHECK (week_number BETWEEN 1 AND 6),
    version INT NOT NULL
        CHECK (version >= 1),
    submitted_at TIMESTAMPTZ NOT NULL,
    session_date DATE NOT NULL,
    attendance JSONB NOT NULL,
    session_notes TEXT
);

CREATE INDEX idx_session_events_natural_key ON session_events (group_id, volunteer_id, week_number);
CREATE INDEX idx_session_events_attendance_gin ON session_events USING GIN (attendance);

COMMENT ON TABLE session_events IS
    'Append-only audit log: one row per submission (every version), never updated. PK is a surrogate event_id, not the natural key — the natural key (group_id, volunteer_id, week_number) can repeat across rows by design, once per version.';
COMMENT ON COLUMN session_events.version IS
    'Assigned by Flink keyed state (max seen for this natural key + 1), not supplied by the client. Never recomputed in Postgres.';
COMMENT ON COLUMN session_events.attendance IS
    'JSONB: per-participant attendance status for this session, e.g. [{"participant_id": "...", "status": "present"}, ...]. Nested/queryable, so JSONB per the project''s JSONB-casting rule.';
COMMENT ON COLUMN session_events.session_notes IS
    'Plain free-text field from the submission. Not nested/queryable, so it stays TEXT rather than JSONB per the project''s JSONB-casting rule.';

-- ----------------------------------------------------------------------------
-- session_state
-- Upsert, stream-derived. One row per (group_id, week_number) reflecting only
-- the highest-version submission seen for that key — the "current" view that
-- mirrors session_events but collapses the version history down to latest.
--
-- Conflict resolution (enforced by the Flink JDBC sink's ON CONFLICT clause,
-- not by a trigger here): on conflict, only overwrite the stored row if the
-- incoming version is strictly greater than session_state.version, with
-- submitted_at as a tiebreaker for the case where two events carrying the
-- same version could otherwise arrive out of order. This guards against a
-- late or null-timestamp event silently clobbering valid state.
-- ----------------------------------------------------------------------------
CREATE TABLE session_state (
    group_id TEXT NOT NULL,
    volunteer_id TEXT NOT NULL,
    week_number SMALLINT NOT NULL
        CHECK (week_number BETWEEN 1 AND 6),
    version INT NOT NULL
        CHECK (version >= 1),
    submitted_at TIMESTAMPTZ NOT NULL,
    session_date DATE NOT NULL,
    attendance JSONB NOT NULL,
    session_notes TEXT,
    PRIMARY KEY (group_id, week_number)
);

CREATE INDEX idx_session_state_volunteer ON session_state (volunteer_id);
CREATE INDEX idx_session_state_attendance_gin ON session_state USING GIN (attendance);

COMMENT ON TABLE session_state IS
    'Upsert sink: one row per (group_id, week_number), always the highest-version submission for that key. Conflict resolution (only overwrite on strictly higher version, submitted_at as tiebreaker) lives in the Flink JDBC sink''s ON CONFLICT clause, not in this DDL.';
COMMENT ON COLUMN session_state.version IS
    'Version of the submission currently reflected here. Only ever moves upward — the JDBC sink''s ON CONFLICT guard rejects incoming rows with version <= the stored value (submitted_at breaks ties on equal versions).';
COMMENT ON COLUMN session_state.attendance IS
    'JSONB: per-participant attendance status for the current (highest-version) submission. Same shape/casting rule as session_events.attendance.';
