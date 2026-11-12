SELECT
    uid AS facility_id,
    facname AS facility_name,
    factype AS facility_type,
    facsubgrp AS facility_subgroup,
    address,
    bbl,
    datasource,
    wkb_geometry AS geom
FROM {{ source('sources', 'dcp_facilities') }}
