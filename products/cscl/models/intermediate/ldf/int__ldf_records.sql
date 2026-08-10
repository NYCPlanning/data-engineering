{{ config(
    materialized = 'table',
    indexes=[ {'columns': ['cumulative_record_number']} ]
) }}

/*
Every LDF data record in publication order, carrying its cumulative record number.

The ordering was taken from GR's published editions, not the Phase III document, which is
wrong on two counts. Record type blocks run N, S, G, P rather than alphabetically, and
within an action code the sort key is (new ID, old ID) for *every* action - including
splits, which the document says sort old-then-new. Verified against 26a and 26b.

Sort keys are built as the fixed-width text the record will hold, with absent IDs as
spaces. That makes a plain text sort reproduce GR's order for free: a space (0x20) sorts
below '0' (0x30), which is exactly how blank IDs come first in their files.
*/

WITH nodes AS (
    SELECT
        1 AS _block,
        record_type,
        action_code,
        x_coord,
        y_coord,
        nodeid,
        destination_x_coord,
        destination_y_coord,
        NULL::int AS old_id,
        NULL::int AS old_from_nodeid,
        NULL::int AS old_to_nodeid,
        NULL::int AS new_id,
        NULL::int AS new_from_nodeid,
        NULL::int AS new_to_nodeid,
        x_coord AS _sort_primary,
        y_coord AS _sort_secondary
    FROM {{ ref('int__ldf_nodes') }}
),

segments AS (
    SELECT
        CASE record_type WHEN 'S' THEN 2 WHEN 'G' THEN 3 WHEN 'P' THEN 4 END AS _block,
        record_type,
        action_code,
        NULL AS x_coord,
        NULL AS y_coord,
        NULL AS nodeid,
        NULL AS destination_x_coord,
        NULL AS destination_y_coord,
        old_id,
        old_from_nodeid,
        old_to_nodeid,
        new_id,
        new_from_nodeid,
        new_to_nodeid,
        coalesce(lpad(new_id::text, 7, '0'), '       ') AS _sort_primary,
        coalesce(lpad(old_id::text, 7, '0'), '       ') AS _sort_secondary
    FROM {{ ref('int__ldf_segments') }}
),

all_records AS (
    SELECT * FROM nodes
    UNION ALL
    SELECT * FROM segments
)

-- The header holds the number this edition starts at; data records continue from it
SELECT
    all_records.*,
    header.cumulative_record_number + row_number() OVER (
        ORDER BY
            all_records._block,
            all_records.action_code,
            all_records._sort_primary,
            all_records._sort_secondary
    ) AS cumulative_record_number
FROM all_records
CROSS JOIN {{ ref('int__ldf_header') }} AS header
