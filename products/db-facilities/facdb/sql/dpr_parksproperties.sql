DROP TABLE IF EXISTS _dpr_parksproperties;

SELECT
    uid,
    source,
    signname as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address,
    NULL as city,
    zipcode,
    boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    typecategory as factype,
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
	END) as facsubgrp,
    'NYC Department of Parks and Recreation' as opname,
    'NYCDPR' as opabbrev,
    'NYCDPR' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _dpr_parksproperties
FROM dpr_parksproperties;

CALL append_to_facdb_base('_dpr_parksproperties');
