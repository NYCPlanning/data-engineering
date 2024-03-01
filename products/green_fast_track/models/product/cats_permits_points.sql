SELECT
    variable_type,
    variable_id,
    permit_geom AS raw_geom
FROM {{ ref('stg__dep_cats_permits') }}
