SELECT DISTINCT
    place_name || ' ' AS place_name,
    place_name || ' ' AS _exception_key
FROM {{ ref('int__exception') }}
