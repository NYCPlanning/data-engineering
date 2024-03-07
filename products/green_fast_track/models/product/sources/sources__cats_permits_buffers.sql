SELECT
    variable_type,
    variable_id,
    buffer
FROM {{ ref('int__dep_cats_permits') }}
