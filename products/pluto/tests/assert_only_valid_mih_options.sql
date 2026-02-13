{{
    config(
        tags = ['de_check', 'minor', 'major'], 
        meta = {
            'description': '''
                This test checks that only the four valid MIH options appear in the source data:
                - Option 1
                - Option 2
                - Deep Affordability Option
                - Workforce Option
                
                Any additional options indicate a source data issue that needs to be investigated.
            ''',
            'next_steps': 'Contact GIS to investigate unexpected MIH option values in source data'
        }
    ) 
}}

WITH valid_options AS (
    SELECT option FROM (VALUES 
        ('Option 1'),
        ('Option 2'),
        ('Option 3'),
        ('Deep Affordability Option'),
        ('Workforce Option')
    ) AS t(option)
),

actual_options AS (
    SELECT * FROM {{ source('build_sources', 'mih_distinct_options') }}
)

-- Return any options that are NOT in the valid list (test fails if any rows returned)
SELECT option
FROM actual_options
WHERE option NOT IN (SELECT option FROM valid_options)
