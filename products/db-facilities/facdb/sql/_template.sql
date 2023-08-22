/*
Note:
1. overabbrev will create overagency and overlevel based ased on lookup
2. facsubgrp will create facdomain, facgroup, servarea based on lookup
3. opabbrev will create optype based on lookup
4. wkb_geometry is the original geometry that came with the csv, usually under the column "wkt"
5. each one of the field should map to a source data column, don't manually
    standardize borough, borough code and etc, we will do that in the end. If a field doesn't exists
    leave it NULL as NULL.
*/
DROP TABLE IF EXISTS _name_of_dataset;

SELECT
    uid,
    source,
    NULL AS facname,
    NULL AS addressnum,
    NULL AS streetname,
    NULL AS address,
    NULL AS city,
    NULL AS zipcode,
    NULL AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    NULL AS factype,
    NULL AS facsubgrp,
    NULL AS opname,
    NULL AS opabbrev,
    NULL AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    NULL AS geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _name_of_dataset
FROM name_of_dataset;

CALL append_to_facdb_base('_dca_operatingbusinesses');
