-- ============================================================================
-- flink_consumer — group_roster seed data
--
-- Plain SQL generator (chosen over a Python script — no extra runtime/deps
-- needed beyond psql, and generate_series makes the fixed cardinality
-- explicit and easy to verify at a glance).
--
-- Generates the fixed roster for one demo cycle:
--   5 districts x 5 volunteers/district x 2 groups/volunteer x 12 participants/group
--   = 600 group_roster rows
--
-- ID scheme (see db/schema.sql header for the full convention):
--   volunteer_id:   <district-code>-V<01-05>            e.g. ADJ-V01
--   group_id:       <volunteer_id>-G<1-2>                e.g. ADJ-V01-G1
--   participant_id: <group_id>-P<01-12>                  e.g. ADJ-V01-G1-P01
--   cycle_id:       CYCLE-01 (single demo cycle)
--
-- Idempotent: safe to re-run against an already-seeded table.
-- ============================================================================

INSERT INTO group_roster (district, volunteer_id, group_id, cycle_id, participant_id)
SELECT
    d.district,
    d.code || '-V' || LPAD(v::TEXT, 2, '0'),
    d.code || '-V' || LPAD(v::TEXT, 2, '0') || '-G' || g::TEXT,
    'CYCLE-01',
    d.code || '-V' || LPAD(v::TEXT, 2, '0') || '-G' || g::TEXT || '-P' || LPAD(p::TEXT, 2, '0')
FROM (VALUES
        ('Adjumani', 'ADJ'),
        ('Masaka', 'MAS'),
        ('Mayuge', 'MAY'),
        ('Moroto', 'MOR'),
        ('Mbarara', 'MBR')
    ) AS d(district, code)
CROSS JOIN generate_series(1, 5) AS v
CROSS JOIN generate_series(1, 2) AS g
CROSS JOIN generate_series(1, 12) AS p
ON CONFLICT (group_id, cycle_id, participant_id) DO NOTHING;
