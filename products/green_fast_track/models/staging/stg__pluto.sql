{{ config(
    materialized = 'table',
    post_hook="CREATE INDEX IF NOT EXISTS stg__pluto_geom_idx ON {{ this }} USING RTREE (geom)"
) }}

WITH mappluto_wi AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_mappluto_wi') }}
),

final AS (
    SELECT
        -- bbl arrives as a DOUBLE in duckdb's parquet archive (a plain text cast in postgres
        -- doesn't add a decimal, but duckdb's DOUBLE->VARCHAR cast does: "3062640072.0" instead
        -- of "3062640072") -- round-tripping through bigint first strips it
        bbl::bigint::text AS bbl,
        zonedist1,
        zonedist2,
        zonedist3,
        zonedist4,
        spdist1,
        spdist2,
        -- empty in the source, so duckdb reads the parquet's null-typed column as
        -- INTEGER and the LIKE in int_flags__zoning fails to bind
        spdist3::varchar AS spdist3,
        landuse,
        {{ dcp_st_transform(dcp_geom_column('dcp_mappluto_wi'), 2263) }} AS geom
    FROM mappluto_wi
)

SELECT * FROM final
