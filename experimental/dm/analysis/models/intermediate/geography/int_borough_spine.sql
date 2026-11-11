SELECT DISTINCT
    borocode AS boro_code,
    boro_name
FROM {{ ref('stg_dcp__cdta2020') }}
