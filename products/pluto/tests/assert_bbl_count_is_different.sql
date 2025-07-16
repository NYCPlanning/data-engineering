{{ 
    config(
        tags = ['de_check', 'major'], 
        meta = {
            'description': '''
				This test checks that final PLUTO table has different row count as previous version PLUTO
                because major versions are expected to use different versions of underlying data (DOF data).
                This test is expected to pass for PLUTO Major versions only.
			''',
            'next_steps': '''
				Investigate if a correct recipe file was used. If so, review stats for input datasets,
                particularly for DOF PTS. 
                This needs to be addressed prior to promoting PLUTO for GIS review.
			'''
        }
    ) 
}}

WITH current_pluto AS (
    SELECT count(*) AS record_count
    FROM {{ source('build_sources', 'export_pluto') }}
),
previous_pluto AS (
    SELECT count(*) AS record_count
    FROM {{ source('build_sources', 'previous_pluto') }}
)

-- query to check if counts are different
SELECT current_pluto.record_count AS total_records
FROM current_pluto, previous_pluto
WHERE current_pluto.record_count = previous_pluto.record_count
