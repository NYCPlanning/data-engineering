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

WITH export_pluto AS (
    SELECT *
    FROM {{ source('build_sources', 'export_pluto') }}
)

SELECT *
FROM export_pluto
WHERE zonedist NOT IN (SELECT zonedist FROM far_lookup)
