{{ 
    config(
        tags = ['de_check', 'minor', 'major'], 
        meta = {
            'description': '''
				This test checks ensures that all zoning districts have defined FAR values in data/dcp_zoning_maxfar.csv
                This could change eventually, if we implement more complete logic around resolving mixed use districts
			''',
            'next_steps': 'Contact Zoning to get updated FAR table'
        }
    ) 
}}

WITH zoning_districts AS (
    SELECT * FROM {{ source('build_sources', 'dcp_zoningdistricts') }}
),

far_lookup AS (
    SELECT * FROM {{ source('build_sources', 'dcp_zoning_maxfar') }}
)

SELECT DISTINCT zonedist
FROM zoning_districts
WHERE zonedist NOT IN (SELECT zonedist FROM far_lookup)
