SELECT
    nta2020,
    ntaname AS nta_name,
    ntaabbrev AS nta_abbreviation,
    ntatype AS nta_type,
    borocode AS boro_code,
    boroname AS boro_name,
    cdta2020,
    shape_area AS shape_area_sqft,
    wkb_geometry AS geom
FROM {{ source('sources', 'dcp_nta2020') }}
