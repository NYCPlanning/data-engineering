SELECT
    boroct2020,
    ct2020,
    ctlabel AS tract_label,
    borocode::integer AS boro_code,
    boroname AS boro_name,
    nta2020,
    ntaname AS nta_name,
    cdta2020,
    puma,
    wkb_geometry AS geom
FROM {{ source('sources', 'dcp_ct2020') }}
