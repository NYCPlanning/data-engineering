{{ config(
    materialized = 'table'
) }}

WITH new_version AS (
    SELECT * FROM {{ ref('int__zoningtaxlots') }}
),

prev_version AS (
    SELECT * FROM {{ source('recipe_sources', 'previous_ztl') }}
),

newnull AS (
    {{ 
        union_newnulls(
            left='new_version',
            right='prev_version',
            cols=[
                'zoning_district_1',
                'zoning_district_2',
                'zoning_district_3',
                'zoning_district_4',
                'commercial_overlay_1',
                'commercial_overlay_2',
                'special_district_1',
                'special_district_2',
                'special_district_3',
                'limited_height_district',
                'zoning_map_number',
                'zoning_map_code'
            ]
        ) 
    }}
),

newvalue AS (
    {{
        union_newvalues(
            left='new_version',
            right='prev_version',
            cols=[
                'zoning_district_1',
                'zoning_district_2',
                'zoning_district_3',
                'zoning_district_4',
                'commercial_overlay_1',
                'commercial_overlay_2',
                'special_district_1',
                'special_district_2',
                'special_district_3',
                'limited_height_district',
                'zoning_map_number',
                'zoning_map_code'
            ]
        )
    }}
),

qaqc_new_nulls AS (
    SELECT
        newnull.field,
        newnull.count AS value_to_null,
        newvalue.count AS null_to_value,
        '{{ env_var('VERSION') }}'::text AS version,
        '{{ env_var('VERSION_PREV') }}'::text AS version_prev
    FROM newnull LEFT JOIN newvalue
        ON newnull.field = newvalue.field
    ORDER BY value_to_null ASC, null_to_value DESC
)

SELECT * FROM qaqc_new_nulls
