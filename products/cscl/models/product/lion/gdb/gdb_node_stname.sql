{{ config(materialized='table') }}

-- Node-to-street-name crosswalk for the published LION GDB (nyclion_*.zip).
-- One row per (node, name): for each node, the distinct principal names of the segments
-- that meet at it. Segments reach a node via their from/to endpoints; each segment's
-- principal name is resolved through its preferred B7SC (int__lgc lgc_rank = 1) into the
-- principal feature-name LOOKUP_KEY. Prod prefixes every STNAME with a single space.
-- Not specified in ETL spec §2.7 (which covers lion/node/altnames); derived empirically
-- and validated against the prod node_stname layer (row/node counts within ~0.02%).
WITH segment_nodes AS (
    SELECT
        segmentid,
        from_nodeid AS nodeid
    FROM {{ ref('int__segments_with_nodes') }}
    WHERE from_nodeid IS NOT null
    UNION
    SELECT
        segmentid,
        to_nodeid AS nodeid
    FROM {{ ref('int__segments_with_nodes') }}
    WHERE to_nodeid IS NOT null
),

-- one preferred B7SC per segment (its principal street/feature)
preferred_b7sc AS (
    SELECT
        segmentid,
        b7sc
    FROM {{ ref('int__lgc') }}
    WHERE lgc_rank = 1
),

-- standard abbreviation for each name's last word (STREET->ST, AVENUE->AVE, ...);
-- words not in the table (BOUNDARY, LINE, ...) are left as-is.
last_word AS (
    SELECT
        upper(full_name) AS full_name,
        min(standard_abbreviation) AS abbr
    FROM {{ source('recipe_sources', 'dcp_cscl_lastword') }}
    WHERE full_name IS NOT null
    GROUP BY 1
),

names AS (
    SELECT
        principal.b7sc,
        ' ' || CASE
            WHEN last_word.abbr IS NOT null
                THEN regexp_replace(principal.lookup_key, '\S+$', last_word.abbr)
            ELSE principal.lookup_key
        END AS stname
    FROM {{ ref('stg__facecode_and_featurename_principal') }} AS principal
    LEFT JOIN last_word
        ON upper((regexp_match(principal.lookup_key, '\S+$'))[1]) = last_word.full_name
)

SELECT DISTINCT
    segment_nodes.nodeid::int AS "NODEID",
    names.stname AS "STNAME"
FROM segment_nodes
INNER JOIN preferred_b7sc ON segment_nodes.segmentid = preferred_b7sc.segmentid
INNER JOIN names ON preferred_b7sc.b7sc = names.b7sc
