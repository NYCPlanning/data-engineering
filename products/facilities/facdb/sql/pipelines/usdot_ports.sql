DROP TABLE IF EXISTS _usdot_ports;
SELECT
    uid,
    source,
    initcap(nav_unit_n) AS facname,
    NULL AS addressnum,
    NULL AS streetname,
    NULL AS address,
    city_or_to AS city,
    zipcode,
    county_nam AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    (CASE
        WHEN nav_unit_n ~* 'Ferry' THEN 'Ferry Landing'
        WHEN nav_unit_n ~* 'Cruise' THEN 'Cruise Terminal'
        ELSE 'Port or Marine Terminal'
    END) AS factype,
    'Ports and Ferry Landings' AS facsubgrp,
    (CASE
        WHEN operators LIKE '%Sanitation%' THEN 'NYC Department of Sanitation'
        WHEN operators LIKE '%Department of Environmental Protection%' THEN 'NYC Department of Environmental Protection'
        WHEN operators LIKE '%Department of Transportation%' THEN 'NYC Department of Transportation'
        WHEN operators LIKE '%Department of Ports and Terminals%' THEN 'NYC Department of Port and Terminals'
        WHEN operators LIKE '%Department of Interior%' THEN 'US Department of Interior'
        WHEN operators LIKE '%Police%' THEN 'NYC Police Department'
        WHEN operators LIKE '%Fire Department%' THEN 'NYC Fire Department'
        WHEN operators LIKE '%Corrections%' THEN 'NYC Department of Correction'
        WHEN operators LIKE '%State University%' THEN 'State University of New York'
        WHEN operators LIKE '%Coast Guard%' THEN 'US Coast Guard'
        WHEN operators IS NULL AND owners LIKE '%Port Authority%' THEN 'Port Authority of New York and New Jersey'
        WHEN operators IS NULL AND owners LIKE '%Parks%' THEN 'NYC Department of Parks and Recreation'
        WHEN
            operators IS NULL AND owners LIKE '%Department of Environmental Protection%'
            THEN 'NYC Department of Environmental Protection'
        WHEN operators IS NULL AND owners LIKE '%Department of Sanitation%' THEN 'NYC Department of Sanitation'
        WHEN operators IS NULL AND owners LIKE '%Department of Transportation%' THEN 'NYC Department of Transportation'
        WHEN
            operators IS NULL AND owners LIKE '%Department of Port and Terminals%'
            THEN 'NYC Department of Port and Terminals'
        WHEN operators IS NULL AND owners LIKE '%Department of Interior%' THEN 'US Department of Interior'
        WHEN operators IS NULL AND owners LIKE '%Police%' THEN 'NYC Police Department'
        WHEN operators IS NULL AND owners LIKE '%Fire Department%' THEN 'NYC Fire Department'
        WHEN operators IS NULL AND owners LIKE '%Corrections%' THEN 'NYC Department of Correction'
        WHEN operators IS NULL AND owners LIKE '%State University%' THEN 'State University of New York'
        WHEN operators IS NULL AND owners LIKE '%Coast Guard%' THEN 'US Coast Guard'
        ELSE 'Non-public'
    END) AS opname,
    (CASE
        WHEN operators LIKE '%Sanitation%' THEN 'NYCDSNY'
        WHEN operators LIKE '%Department of Environmental Protection%' THEN 'NYCDEP'
        WHEN operators LIKE '%Department of Transportation%' THEN 'NYCDOT'
        WHEN operators LIKE '%Department of Ports and Terminals%' THEN 'NYCDPT'
        WHEN operators LIKE '%Department of Interior%' THEN 'USDOI'
        WHEN operators LIKE '%Police%' THEN 'NYCNYPD'
        WHEN operators LIKE '%Fire Department%' THEN 'NYCFDNY'
        WHEN operators LIKE '%Corrections%' THEN 'NYCDOC'
        WHEN operators LIKE '%State University%' THEN 'SUNY'
        WHEN operators LIKE '%Coast Guard%' THEN 'USCG'
        WHEN operators IS NULL AND owners LIKE '%Port Authority%' THEN 'PANYNJ'
        WHEN operators IS NULL AND owners LIKE '%Economic Development%' THEN 'NYCEDC'
        WHEN operators IS NULL AND owners LIKE '%Parks%' THEN 'NYCDPR'
        WHEN operators IS NULL AND owners LIKE '%Department of Environmental Protection%' THEN 'NYCDEP'
        WHEN operators IS NULL AND owners LIKE '%Sanitation%' THEN 'NYCDSNY'
        WHEN operators IS NULL AND owners LIKE '%Transportation%' THEN 'NYCDOT'
        WHEN operators IS NULL AND owners LIKE '%Department of Port and Terminals%' THEN 'NYCDPT'
        WHEN operators IS NULL AND owners LIKE '%Department of Interior%' THEN 'USDOI'
        WHEN operators IS NULL AND owners LIKE '%Police%' THEN 'NYCNYPD'
        WHEN operators IS NULL AND owners LIKE '%Fire Department%' THEN 'NYCFDNY'
        WHEN operators IS NULL AND owners LIKE '%Corrections%' THEN 'NYCDOC'
        WHEN operators IS NULL AND owners LIKE '%State University%' THEN 'SUNY'
        WHEN operators IS NULL AND owners LIKE '%Coast Guard%' THEN 'USCG'
        ELSE 'Non-public'
    END) AS opabbrev,
    (CASE
        WHEN owners LIKE '%Port Authority%' THEN 'PANYNJ'
        WHEN owners LIKE '%Economic Development%' THEN 'NYCEDC'
        WHEN owners LIKE '%Parks%' THEN 'NYCDPR'
        WHEN owners LIKE '%Department of Environmental Protection%' THEN 'NYCDEP'
        WHEN owners LIKE '%Department of Transportation%' THEN 'NYCDOT'
        WHEN owners LIKE '%Department of Port and Terminals%' THEN 'NYCDPT'
        WHEN owners LIKE '%Department of Interior%' THEN 'USDOI'
        WHEN owners LIKE '%Police%' THEN 'NYCNYPD'
        WHEN owners LIKE '%Fire Department%' THEN 'NYCFDNY'
        WHEN owners LIKE '%Corrections%' THEN 'NYCDOC'
        WHEN owners LIKE '%State University%' THEN 'SUNY'
        WHEN owners LIKE '%Coast Guard%' THEN 'USCG'
        WHEN owners LIKE '%United States%' THEN 'USCG'
        WHEN owners = 'Current Owner: City of New York.' THEN 'NYC-Unknown'
        WHEN operators LIKE '%Sanitation%' THEN 'NYCDSNY'
        WHEN operators LIKE '%Department of Environmental Protection%' THEN 'NYCDEP'
        WHEN operators LIKE '%Department of Sanitation%' THEN 'NYCDOT'
        WHEN operators LIKE '%Department of Ports and Terminals%' THEN 'NYCDPT'
        WHEN operators LIKE '%Department of Interior%' THEN 'USDOI'
        WHEN operators LIKE '%Police%' THEN 'NYCNYPD'
        WHEN operators LIKE '%Fire Department%' THEN 'NYCFDNY'
        WHEN operators LIKE '%Corrections%' THEN 'NYCDOC'
        WHEN operators LIKE '%State University%' THEN 'SUNY'
        WHEN operators LIKE '%Coast Guard%' THEN 'USCG'
        ELSE 'Non-public'
    END) AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    geometry AS wkb_geometry,
    NULL AS geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _usdot_ports
FROM usdot_ports
WHERE nav_unit_n ~* '^port|terminal|ferry|cruise';

CALL append_to_facdb_base('_usdot_ports');
