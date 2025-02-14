{{ 
    config(
        tags = ['de_check', 'minor'], 
        meta = {
            'description': '''
				This test checks that final PLUTO table has same row count as previous version PLUTO. 
                This test is expected to pass for PLUTO Minor versions only.
			''',
            'next_steps': '''
				Investigate if a correct recipe file was used. If so, review stats for input datasets. 
                This needs to be addressed prior to promoting PLUTO for GIS review.
			'''
        }
    ) 
}}

WITH current_pluto AS (
    SELECT
        bbl::bigint,
        'current' AS source
    FROM {{ source('build_sources', 'export_pluto') }}
),
previous_pluto AS (
    SELECT
        bbl::decimal::bigint,
        'previous' AS source
    FROM {{ source('build_sources', 'previous_pluto') }}
)

-- query to detect differences and isolate records absent in one of the tables
SELECT
    COALESCE(current_pluto.bbl, previous_pluto.bbl) AS bbl,
    COALESCE(current_pluto.source, previous_pluto.source) AS source
FROM current_pluto
FULL OUTER JOIN previous_pluto
    ON current_pluto.bbl = previous_pluto.bbl
WHERE
    current_pluto.bbl IS NULL
    OR previous_pluto.bbl IS NULL
ORDER BY source
