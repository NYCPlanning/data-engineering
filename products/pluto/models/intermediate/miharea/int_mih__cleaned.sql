{{
    config(
        materialized='table',
        tags=['pluto_enrichment']
    )
}}

-- Clean MIH option names and create unique identifiers
-- Handles typos and standardizes formatting

SELECT
    project_id || '-' || mih_option AS mih_id,
    *,
    TRIM(
        -- Step 2b: collapse any sequence of commas (e.g., ",,", ",,,")
        REGEXP_REPLACE(
            -- Step 2a: Replace "and" or "," (with any spaces) with a single comma
            REGEXP_REPLACE(
                -- Step 1: Add space between "Option" and number
                REGEXP_REPLACE(
                    REPLACE(mih_option, 'Affordablility', 'Affordability'),
                    'Option(\d)',
                    'Option \1',
                    'g'
                ),
                '\s*(,|and)\s*',
                ',',
                'g'
            ),
            ',+',
            ',',
            'g'
        ),
        ', '
    ) AS cleaned_option
FROM {{ ref('stg__dcp_gis_mandatory_inclusionary_housing') }}
