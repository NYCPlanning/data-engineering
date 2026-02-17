SELECT DISTINCT place_name || ' ' AS place_name
FROM {{ ref('int__exception') }}
