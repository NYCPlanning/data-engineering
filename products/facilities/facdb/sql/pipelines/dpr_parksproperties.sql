DROP TABLE IF EXISTS _dpr_parksproperties;

SELECT
    uid,
    source,
    signname AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address,
    NULL AS city,
    zipcode,
    boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    typecategory AS factype,
    (CASE
        -- admin of gov
        WHEN typecategory = 'Lot' THEN 'City Agency Parking'
        -- parks
        WHEN typecategory = 'Cemetery' THEN 'Cemeteries'
        WHEN typecategory = 'Historic House Park' THEN 'Historical Sites'
        WHEN typecategory = 'Triangle/Plaza' THEN 'Streetscapes, Plazas, and Malls'
        WHEN typecategory = 'Mall' THEN 'Streetscapes, Plazas, and Malls'
        WHEN typecategory = 'Strip' THEN 'Streetscapes, Plazas, and Malls'
        WHEN typecategory = 'Parkway' THEN 'Streetscapes, Plazas, and Malls'
        WHEN typecategory = 'Tracking' THEN 'Streetscapes, Plazas, and Malls'
        WHEN typecategory = 'Garden' THEN 'Gardens'
        WHEN typecategory = 'Nature Area' THEN 'Preserves and Conservation Areas'
        WHEN typecategory = 'Flagship Park' THEN 'Parks'
        WHEN typecategory = 'Community Park' THEN 'Parks'
        WHEN typecategory = 'Neighborhood Park' THEN 'Parks'
        WHEN typecategory = 'Undeveloped' THEN 'Undeveloped'
        ELSE 'Recreation and Waterfront Sites'
    END) AS facsubgrp,
    'NYC Department of Parks and Recreation' AS opname,
    'NYCDPR' AS opabbrev,
    'NYCDPR' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _dpr_parksproperties
FROM dpr_parksproperties;

CALL append_to_facdb_base('_dpr_parksproperties');
