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
    NULL as facname,
    NULL as addressnum,
    NULL as streetname,
    NULL as address,
    NULL as city,
    NULL as zipcode,
    NULL as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    NULL as factype,
    NULL as facsubgrp,
    NULL as opname,
    NULL as opabbrev,
    NULL as overabbrev,
    NULL as capacity,
    NULL as captype,
    NULL as wkb_geometry,
    NULL as geo_1b,
    NULL as geo_bl,
    NULL as geo_bn
INTO _name_of_dataset
FROM name_of_dataset;

CALL append_to_facdb_base('_dca_operatingbusinesses');
