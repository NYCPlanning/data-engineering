{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['record_type']},
      {'columns': ['old_id']},
      {'columns': ['new_id']},
    ]
) }}

/*
The LDF's segment records: 'S' (base centerline), 'P' (physical) and 'G' (generic).

Unlike node records these are not derived by diffing LION. CSCL journals every centerline
edit in CENTERLINEHISTORY and the LDF publishes that journal, so two things happen here:

1. Select the edition. Rows for the release being cut carry no release_num - GR stamps it
   at publish time, after the GDB we ingest was snapshotted.
2. Eliminate transitory records. A segment created and destroyed between two releases was
   never visible to LION users, so its whole lineage is dropped. GR's rule for this lives
   in an assembly we don't have; we approximate it by dropping a lineage component when
   none of its IDs surface in either LION release. See data_issues.md CSCL-LDF-01 for how
   close that gets and what constrains improving it.
*/

WITH RECURSIVE pending AS (
    SELECT
        record_type,
        action_code,
        old_segment_id AS old_id,
        old_from_nodeid,
        old_to_nodeid,
        new_segment_id AS new_id,
        new_from_nodeid,
        new_to_nodeid
    FROM {{ ref('stg__centerlinehistory') }}
    WHERE release_num IS NULL AND record_type IN ('S', 'P', 'G')
),

lion_rows AS (
    SELECT
        segmentid,
        physicalid,
        genericid
    FROM {{ source('production_outputs', 'previous_citywide_lion_dat') }}
    UNION ALL
    SELECT
        segmentid,
        physicalid,
        genericid
    FROM {{ ref('lion_dat_by_field') }}
),

-- Every ID that surfaced in either release, keyed the same way as the journal's
-- record_type: 'S' tracks segment IDs, 'P' physical IDs, 'G' generic IDs.
-- The regex guard is not cosmetic: these fields are zero-filled text, and a stray '-1'
-- sentinel arrives as '00000-1', which a bare cast would choke on.
published_ids AS (
    SELECT
        'S' AS record_type,
        CASE WHEN trim(segmentid) ~ '^[0-9]+$' THEN trim(segmentid)::int END AS id
    FROM lion_rows
    UNION
    SELECT
        'P' AS record_type,
        CASE WHEN trim(physicalid) ~ '^[0-9]+$' THEN trim(physicalid)::int END AS id
    FROM lion_rows
    UNION
    SELECT
        'G' AS record_type,
        CASE WHEN trim(genericid) ~ '^[0-9]+$' THEN trim(genericid)::int END AS id
    FROM lion_rows
),

journal_ids AS (
    SELECT
        record_type,
        old_id AS id
    FROM pending
    WHERE old_id IS NOT NULL
    UNION
    SELECT
        record_type,
        new_id
    FROM pending
    WHERE new_id IS NOT NULL
),

-- A split/merge/renumber links an old ID to a new one; treat those links as undirected
-- so a whole lineage is judged together rather than record by record.
adjacency AS (
    SELECT
        record_type,
        old_id AS id,
        new_id AS neighbor
    FROM pending
    WHERE old_id IS NOT NULL AND new_id IS NOT NULL
    UNION
    SELECT
        record_type,
        new_id,
        old_id
    FROM pending
    WHERE old_id IS NOT NULL AND new_id IS NOT NULL
),

-- Label propagation to connected components. Components are tiny (a handful of IDs), so
-- this settles in a couple of iterations.
labelled AS (
    SELECT
        record_type,
        id,
        id AS label
    FROM journal_ids
    UNION
    SELECT
        labelled.record_type,
        adjacency.neighbor,
        labelled.label
    FROM labelled
    INNER JOIN adjacency
        ON
            labelled.record_type = adjacency.record_type
            AND labelled.id = adjacency.id
),

component_of AS (
    SELECT
        record_type,
        id,
        min(label) AS component
    FROM labelled
    GROUP BY record_type, id
),

live_components AS (
    SELECT DISTINCT
        component_of.record_type,
        component_of.component
    FROM component_of
    INNER JOIN published_ids
        ON
            component_of.record_type = published_ids.record_type
            AND component_of.id = published_ids.id
),

-- Kept as a plain set rather than a correlated EXISTS: the filter below is a disjunction,
-- which stops the planner turning a subquery into a semi-join and makes it rescan.
live_ids AS (
    SELECT
        component_of.record_type,
        component_of.id
    FROM component_of
    INNER JOIN live_components
        ON
            component_of.record_type = live_components.record_type
            AND component_of.component = live_components.component
)

SELECT
    pending.record_type,
    pending.action_code,
    pending.old_id,
    pending.old_from_nodeid,
    pending.old_to_nodeid,
    pending.new_id,
    pending.new_from_nodeid,
    pending.new_to_nodeid
FROM pending
LEFT JOIN live_ids AS old_live
    ON
        pending.record_type = old_live.record_type
        AND pending.old_id = old_live.id
LEFT JOIN live_ids AS new_live
    ON
        pending.record_type = new_live.record_type
        AND pending.new_id = new_live.id
WHERE
    old_live.id IS NOT NULL
    OR new_live.id IS NOT NULL
    -- Some records carry only a node pair and no IDs at all. There is no lineage to
    -- judge them by, so they pass through untouched.
    OR (pending.old_id IS NULL AND pending.new_id IS NULL)
