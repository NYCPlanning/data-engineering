{{ config(materialized='table') }}

-- Node-to-street-name crosswalk for the published LION GDB (nyclion_*.zip).
-- One row per (node, name): for each node, the distinct principal names of the segments
-- that meet at it. Segments reach a node via their from/to endpoints; each segment's
-- principal name is resolved through its preferred B7SC (int__lgc lgc_rank = 1) into the
-- principal feature-name LOOKUP_KEY. Prod prefixes every STNAME with a single space.
-- Not specified in ETL spec §2.7 (which covers lion/node/altnames); derived empirically
-- and validated against the prod node_stname layer (row/node counts within ~0.02%).
--
-- Two abbreviation steps, both empirically derived by finding, for each dev/prod node
-- with a shared name prefix, what suffix prod actually used - see CSCL-LION-09 in
-- data_issues.md for the full derivation and its limits (it materially reduces but does
-- not eliminate node_stname's dev/prod mismatch - some remaining cases are genuinely
-- contextual, e.g. "WEST" is abbreviated in "WEST 42 ST" but not in "WEST FARMS RD", which
-- a word-level rule can't distinguish):
--   - Last word: dcp_cscl_lastword's standard_abbreviation is right most of the time, but
--     wrong or entirely missing for ~26 words (seeds/node_stname_lastword_overrides.csv
--     corrects those - some map to a *different* dcp_cscl_lastword column, e.g. COURT's
--     real abbreviation is its shortest_abbreviation "CT", not standard_abbreviation
--     "CRT"; others, e.g. CAMP -> CP, aren't in any dcp_cscl_lastword column at all).
--   - First word: prod also abbreviates directional prefixes (EAST -> E, "EAST 174 ST" ->
--     "E 174 ST") via dcp_cscl_universalword, restricted to seeds/directional_indicators.csv
--     - trying this for every dcp_cscl_universalword match (not just directional words)
--     looked promising in isolation but wrongly abbreviated standalone names prod never
--     touches (e.g. "BROADWAY" -> "BDWY"), so it's intentionally narrow.
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

-- standard abbreviation for each name's last word (STREET->ST, AVENUE->AVE, ...), with
-- empirically-derived corrections layered on - see seeds/node_stname_lastword_overrides.csv.
-- words not in the table (BOUNDARY, LINE, ...) are left as-is.
last_word AS (
    SELECT
        upper(lastword.full_name) AS full_name,
        coalesce(overrides.abbr, min(lastword.standard_abbreviation)) AS abbr
    FROM {{ source('recipe_sources', 'dcp_cscl_lastword') }} AS lastword
    LEFT JOIN {{ ref('node_stname_lastword_overrides') }} AS overrides
        ON upper(lastword.full_name) = upper(overrides.full_name)
    WHERE lastword.full_name IS NOT null
    GROUP BY upper(lastword.full_name), overrides.abbr
),

-- standard abbreviation for a name's first word, restricted to directional prefixes
-- (EAST/WEST/NORTH/SOUTH and their -BOUND/compass variants) - see module note above.
first_word AS (
    SELECT
        upper(universalword.full_name) AS full_name,
        min(universalword.standard_abbreviation) AS abbr
    FROM {{ source('recipe_sources', 'dcp_cscl_universalword') }} AS universalword
    INNER JOIN {{ ref('directional_indicators') }} AS directional
        ON upper(universalword.full_name) = upper(directional.word)
    WHERE universalword.full_name IS NOT null
    GROUP BY upper(universalword.full_name)
),

names AS (
    SELECT
        principal.b7sc,
        ' ' || CASE
            WHEN first_word.abbr IS NOT null AND last_word.abbr IS NOT null
                THEN regexp_replace(
                    regexp_replace(principal.lookup_key, '\S+$', last_word.abbr),
                    '^\S+', first_word.abbr
                )
            WHEN first_word.abbr IS NOT null
                THEN regexp_replace(principal.lookup_key, '^\S+', first_word.abbr)
            WHEN last_word.abbr IS NOT null
                THEN regexp_replace(principal.lookup_key, '\S+$', last_word.abbr)
            ELSE principal.lookup_key
        END AS stname
    FROM {{ ref('stg__facecode_and_featurename_principal') }} AS principal
    LEFT JOIN last_word
        ON upper((regexp_match(principal.lookup_key, '\S+$'))[1]) = last_word.full_name
    LEFT JOIN first_word
        ON upper((regexp_match(principal.lookup_key, '^\S+'))[1]) = first_word.full_name
)

SELECT DISTINCT
    segment_nodes.nodeid::int AS "NODEID",
    names.stname AS "STNAME"
FROM segment_nodes
INNER JOIN preferred_b7sc ON segment_nodes.segmentid = preferred_b7sc.segmentid
INNER JOIN names ON preferred_b7sc.b7sc = names.b7sc
