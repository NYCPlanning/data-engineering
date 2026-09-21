{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Borough is assigned by point-on-surface containment per the ETL spec's centroid
-- rule; point-on-surface keeps the probe inside concave shapes.
-- CDTA is matched the same way.
--
-- d.geom is bounded to its own assigned borough (shrunk 5 ft) before shoreline
-- clipping - see gdb_nynta2010.sql and CSCL-DISTRICTS-03 for why (a genuine
-- AtomicPolygon coverage void past legal/jurisdictional lines like the NY/NJ state
-- line, plus at least one real stg__neighborhood/stg__nta2020-family data error
-- caught the same way).

WITH bounded AS (
    SELECT
        b.borocode::int AS "BoroCode",
        b.boroname AS "BoroName",
        b.fips AS "CountyFIPS",
        d.neighborhood_code AS "NTA2020",
        nta.nta_name AS "NTAName",
        nta.nta_abbrev AS "NTAAbbrev",
        nta.nta_type AS "NTAType",
        cdta.cdta_code AS "CDTA2020",
        cdta_equiv.cdta_name AS "CDTAName",
        st_intersection(d.geom, st_buffer(b.geom, -5)) AS geom
    FROM {{ ref('stg__nta2020') }} AS d
    INNER JOIN {{ ref('stg__borough') }} AS b
        ON st_contains(b.geom, st_pointonsurface(d.geom))
    LEFT JOIN {{ ref('stg__ntaequiv2020') }} AS nta ON d.neighborhood_code = nta.nta_code
    LEFT JOIN {{ ref('stg__cdta2020') }} AS cdta
        ON st_contains(cdta.geom, st_pointonsurface(d.geom))
    LEFT JOIN {{ ref('stg__cdtaequiv2020') }} AS cdta_equiv
        ON cdta.cdta_code = cdta_equiv.cdta_code
),

clipped AS (
    SELECT
        "BoroCode",
        "BoroName",
        "CountyFIPS",
        "NTA2020",
        "NTAName",
        "NTAAbbrev",
        "NTAType",
        "CDTA2020",
        "CDTAName",
        {{ clipped_geom('bounded.geom') }} AS geom
    FROM bounded
    {{ clip_to_shoreline('bounded.geom') }}
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
