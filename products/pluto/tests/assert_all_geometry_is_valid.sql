{{ 
    config(
        tags = ['de_check', 'minor', 'major'], 
        meta = {
            'description': '''
				This test checks for any invalid lot geometries
			''',
            'next_steps': 'Contact DE and GIS to investigate'
        }
    ) 
}}

WITH pluto_geom AS (
    SELECT
        bbl,
        geom_2263,
        geom_4326,
        clipped_2263,
        clipped_4326
    FROM {{ source('build_sources', 'pluto_geom') }}
),

-- ST_IsValidDetail returns a row containing:
--      a boolean (valid) stating if a geometry is valid
--      a varchar (reason) stating why it is invalid
--      a geometry (location) pointing out where it is invalid
-- https://postgis.net/docs/ST_IsValidDetail.html
validity_details AS (
    SELECT
        bbl,
        ST_ISVALIDDETAIL(geom_2263) AS validity_details_geom_2263,
        ST_ISVALIDDETAIL(geom_4326) AS validity_details_geom_4326,
        ST_ISVALIDDETAIL(clipped_2263) AS validity_details_clipped_2263,
        ST_ISVALIDDETAIL(clipped_4326) AS validity_details_clipped_4326
    FROM pluto_geom
),

check_all_geoms AS (
    SELECT
        *,
        (
            false
            = ANY(
                ARRAY[
                    (validity_details_geom_2263).valid,
                    (validity_details_geom_4326).valid,
                    (validity_details_clipped_2263).valid,
                    (validity_details_clipped_4326).valid
                ]
            )
        ) AS any_geoms_invalid
    FROM validity_details
)

SELECT * FROM check_all_geoms
WHERE any_geoms_invalid
