{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- 2020 neighborhood tabulation areas. Boundaries come from the AtomicPolygon topology
-- (int__boundary__nta2020); columns, column order and types match the published layer.

-- borough holding most of the district's AP area - the borough it physically sits in
-- (Rikers Island's NTA QN0151 belongs to a Queens CDTA but is in the Bronx), robust to a
-- few APs across a borough line (QN99, Queens parks and cemeteries, has Brooklyn APs)
WITH district_borough AS (
    SELECT DISTINCT ON (m.nta2020)  -- noqa: AM01
        m.nta2020 AS entity_id,
        m.borocode
    FROM {{ ref('int__topology__ap_entities') }} AS m
    WHERE m.nta2020 IS NOT NULL
    GROUP BY m.nta2020, m.borocode
    ORDER BY m.nta2020, sum(m.area_sqft) DESC
),

-- every NTA nests in exactly one CDTA (via its 2020 tracts)
nta_cdta AS (
    SELECT
        nta2020 AS entity_id,
        min(cdta2020) AS cdta2020
    FROM {{ ref('int__topology__ap_entities') }}
    WHERE nta2020 IS NOT NULL
    GROUP BY nta2020
),

boundaries AS (
    SELECT
        entity_id,
        st_multi(geom) AS geom
    FROM {{ ref('int__boundary__nta2020') }}
)

SELECT
    db.borocode::smallint AS "BoroCode",
    boro.boroname::varchar(13) AS "BoroName",
    boro.fips::varchar(3) AS "CountyFIPS",
    b.entity_id::varchar(6) AS "NTA2020",
    nta.nta_name::varchar(75) AS "NTAName",
    nta.nta_abbrev::varchar(10) AS "NTAAbbrev",
    nta.nta_type::varchar(1) AS "NTAType",
    nc.cdta2020::varchar(4) AS "CDTA2020",
    cdta.cdta_name::varchar(75) AS "CDTAName",
    b.geom,
    st_perimeter(b.geom) AS "SHAPE_Length",
    st_area(b.geom) AS "SHAPE_Area"
FROM boundaries AS b
LEFT JOIN district_borough AS db ON b.entity_id = db.entity_id
LEFT JOIN {{ ref('stg__borough') }} AS boro ON db.borocode = boro.borocode
LEFT JOIN {{ ref('stg__ntaequiv2020') }} AS nta ON b.entity_id = nta.nta_code
LEFT JOIN nta_cdta AS nc ON b.entity_id = nc.entity_id
LEFT JOIN {{ ref('stg__cdtaequiv2020') }} AS cdta ON nc.cdta2020 = cdta.cdta_code
