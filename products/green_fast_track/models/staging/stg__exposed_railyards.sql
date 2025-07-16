WITH cscl_commonplace AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_commonplace') }}
),

cscl_complex AS (
    SELECT * FROM {{ source('recipe_sources', 'dcp_cscl_complex') }}
),

hudson_correction AS (
    SELECT * FROM {{ ref('railyards_hudsonyards_erase') }}
),

joined_and_corrected AS (
    SELECT
        p.feature_name AS variable_id,
        c.objectid AS c_objectid,
        c.complexid AS c_complexid,
        c.name,
        CASE
            WHEN hudson_correction.complexid IS NULL THEN c.wkb_geometry
            ELSE ST_Difference(c.wkb_geometry, hudson_correction.geom, 1)
        END AS raw_geom
    FROM cscl_commonplace AS p
    INNER JOIN cscl_complex AS c ON p.complexid = c.complexid
    LEFT JOIN hudson_correction ON p.complexid = hudson_correction.complexid
    WHERE facility_type = 6 AND facility_domains = '3' AND saftype = 'G'
)

SELECT
    'exposed_railyards' AS variable_type,
    variable_id,
    ST_Multi(raw_geom) AS raw_geom
FROM
    joined_and_corrected
