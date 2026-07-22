{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Borough is assigned by point-on-surface containment per the ETL spec's centroid
-- rule; point-on-surface keeps the probe inside concave shapes.

WITH clipped AS (
    SELECT
        b.borocode::int AS "BoroCode",
        b.boroname AS "BoroName",
        b.fips AS "CountyFIPS",
        d.cdta_code AS "CDTA2020",
        cdta.cdta_name AS "CDTAName",
        cdta.cdta_type AS "CDTAType",
        {{ clipped_geom('d.geom') }} AS geom
    FROM {{ ref('stg__cdta2020') }} AS d
    INNER JOIN {{ ref('stg__borough') }} AS b
        ON st_contains(b.geom, st_pointonsurface(d.geom))
    LEFT JOIN {{ ref('stg__cdtaequiv2020') }} AS cdta ON d.cdta_code = cdta.cdta_code
    {{ clip_to_shoreline('d.geom') }}
)

SELECT
    *,
    st_perimeter(geom) AS "SHAPE_Length",
    st_area(geom) AS "SHAPE_Area"
FROM clipped
WHERE NOT st_isempty(geom)
