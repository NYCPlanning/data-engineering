{{
    config(
        tags = ['de_check'],
        severity = 'warn',
        meta = {
            'description': '''
                HPD sometimes publishes one physical project under two project_ids — e.g. HNY
                44223 "ROCHESTER SUYDAM PHASE 1" and 70913 "ROCHESTER SUYDAM PHASE I", same
                BBLs and unit counts. When both copies survive matching, _hny_match.sql flags
                the job one-dev-to-many-hny and SUMs the unit fields, so classa_hnyaff comes
                out at 2x the real affordable unit count.

                Neither existing safeguard catches this. The match-priority filter only
                deduplicates the copies when they score differently; the hny_corrections guard
                only checks whether that exact (hny_id, job_number) pair is already present,
                not whether the job already has a match for the same building under another
                project_id. So corrections can reintroduce a copy the filter dropped, and two
                equally well-geocoded copies defeat both.

                Flagged pairs share a BBL and total_units, and either share all_counted_units
                or have it null on both sides. Pairs whose all_counted_units genuinely differ
                are excluded — those are usually real phases, where summing is correct.
            ''',
            'next_steps': '''
                1. Confirm the two HNY records are the same buildings, not real phases:
                   compare project_name, address and project_completion_date in hny_geo.
                2. If they are duplicates, decide which copy is canonical. Prefer the one whose
                   geo_bin is a real BIN over a borough placeholder (1000000, 2000000, ...);
                   where both geocode cleanly there is no signal and it needs a human call.
                3. If from_corrections is true for the copy being dropped, remove that row from
                   data/corrections/hny_corrections.csv. If both copies came from automated
                   matching, the corrections file cannot fix it — escalate as a source data
                   issue and record the duplicate project_id.
            '''
        }
    )
}}

-- DISTINCT because hny_matches can hold exact duplicate rows: the INSERT applying
-- 'add' corrections tests each row against the statement-start snapshot, so a pair
-- listed twice in hny_corrections.csv is inserted twice.
WITH matched_buildings AS (
    SELECT DISTINCT
        m.job_number,
        m.hny_id,
        m.hny_project_id,
        g.project_name,
        g.geo_bbl,
        nullif(btrim(g.total_units), '')::numeric AS total_units,
        nullif(btrim(g.all_counted_units), '')::numeric AS all_counted_units,
        -- btrim because some rows in the corrections CSV carry a trailing space in action
        (m.hny_id, m.job_number) IN (
            SELECT
                c.hny_id,
                c.job_number
            FROM {{ source('build_sources', 'hny_corrections') }} AS c
            WHERE btrim(c.action) = 'add'
        ) AS from_corrections
    FROM {{ source('build_sources', 'hny_matches') }} AS m
    INNER JOIN {{ source('build_sources', 'hny_geo') }} AS g
        ON m.hny_id = g.hny_id
)

SELECT
    a.job_number,
    a.geo_bbl,
    a.total_units,
    a.all_counted_units AS units_per_copy,
    a.hny_id AS hny_id_1,
    a.project_name AS project_name_1,
    a.from_corrections AS from_corrections_1,
    b.hny_id AS hny_id_2,
    b.project_name AS project_name_2,
    b.from_corrections AS from_corrections_2
FROM matched_buildings AS a
INNER JOIN matched_buildings AS b
    ON
        a.job_number = b.job_number
        -- Emit one row per unordered pair rather than both orderings
        AND a.hny_project_id < b.hny_project_id
        AND a.geo_bbl IS NOT DISTINCT FROM b.geo_bbl
        -- total_units must be present and equal: with both sides NULL the only evidence
        -- left is a shared BBL, which two genuinely different buildings can also have.
        -- all_counted_units is allowed to be NULL on both sides once totals already agree.
        AND a.total_units = b.total_units
        AND a.all_counted_units IS NOT DISTINCT FROM b.all_counted_units
ORDER BY a.job_number
