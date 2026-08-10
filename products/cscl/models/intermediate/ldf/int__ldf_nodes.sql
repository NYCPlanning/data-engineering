{{ config(
    materialized = 'table',
    indexes=[ {'columns': ['nodeid']} ]
) }}

/*
The LDF's node ('N') records. Unlike the segment records, these are not journalled by
CSCL - they are derived by diffing the previous LION release against this one, which is
what GR's extract tool does.

Both sides are read in their LION representation rather than from CSCL node geometry, so
the coordinates are byte-identical to what each release published.
*/

WITH previous_nodes AS (
    SELECT
        from_nodeid AS nodeid,
        from_x AS x_coord,
        from_y AS y_coord
    FROM {{ source('production_outputs', 'previous_citywide_lion_dat') }}
    UNION
    SELECT
        to_nodeid,
        to_x,
        to_y
    FROM {{ source('production_outputs', 'previous_citywide_lion_dat') }}
),

current_nodes AS (
    SELECT
        from_nodeid AS nodeid,
        from_x AS x_coord,
        from_y AS y_coord
    FROM {{ ref('lion_dat_by_field') }}
    UNION
    SELECT
        to_nodeid,
        to_x,
        to_y
    FROM {{ ref('lion_dat_by_field') }}
)

SELECT
    'N' AS record_type,
    CASE
        WHEN previous.nodeid IS NULL THEN 'A'
        WHEN current.nodeid IS NULL THEN 'D'
        ELSE 'M'
    END AS action_code,
    coalesce(previous.nodeid, current.nodeid) AS nodeid,
    -- Adds carry the new location here; deletes and moves carry the old one
    coalesce(previous.x_coord, current.x_coord) AS x_coord,
    coalesce(previous.y_coord, current.y_coord) AS y_coord,
    -- Populated for moves only
    CASE WHEN previous.nodeid IS NOT NULL THEN current.x_coord END AS destination_x_coord,
    CASE WHEN previous.nodeid IS NOT NULL THEN current.y_coord END AS destination_y_coord
FROM previous_nodes AS previous
FULL OUTER JOIN current_nodes AS current
    ON previous.nodeid = current.nodeid
WHERE
    previous.nodeid IS NULL
    OR current.nodeid IS NULL
    OR (previous.x_coord, previous.y_coord)
    IS DISTINCT FROM (current.x_coord, current.y_coord)
