{{ config(
    materialized='table',
    indexes=[{'columns': ['geom'], 'type': 'gist'}]
) }}

-- Minor census economic areas. Boundaries come from the AtomicPolygon topology, grouped
-- into the published rows by int__boundary__mcea_rows; columns, column order, types and
-- rows match the published layer.

-- borough holding most of the district's AP area - the borough it physically sits in
-- (Rikers Island's NTA QN0151 belongs to a Queens CDTA but is in the Bronx), robust to a
-- few APs across a borough line (QN99, Queens parks and cemeteries, has Brooklyn APs)
WITH district_borough AS (
    SELECT DISTINCT ON (m.mcea)  -- noqa: AM01
        m.mcea AS entity_id,
        m.borocode
    FROM {{ ref('int__topology__ap_entities') }} AS m
    WHERE m.mcea IS NOT NULL
    GROUP BY m.mcea, m.borocode
    ORDER BY m.mcea, sum(m.area_sqft) DESC
)

-- prod spells these mixed-case on this layer alone; every other feature class
-- uses SHAPE_Length/SHAPE_Area
SELECT
    db.borocode::varchar(1) AS "BOROCODE",
    s.entity_id::varchar(20) AS "MCEA",
    boro.boroname::varchar(32) AS "BoroName",
    s.geom,
    st_perimeter(s.geom) AS "Shape_Length",
    st_area(s.geom) AS "Shape_Area"
FROM {{ ref('int__boundary__mcea_rows') }} AS s
LEFT JOIN district_borough AS db ON s.entity_id = db.entity_id
LEFT JOIN {{ ref('stg__borough') }} AS boro ON db.borocode = boro.borocode
