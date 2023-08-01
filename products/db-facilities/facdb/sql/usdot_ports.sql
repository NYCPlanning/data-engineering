DROP TABLE IF EXISTS _usdot_ports;
SELECT
    uid,
    source,
    initcap(nav_unit_n) as facname,
    NULL as addressnum,
    NULL as streetname,
    NULL as address,
    city_or_to as city,
    zipcode,
    county_nam as boro,
    NULL as borocode,
    NULL as bin,
    NULL as bbl,
    (CASE
		WHEN nav_unit_n ~* 'Ferry' THEN 'Ferry Landing'
		WHEN nav_unit_n ~* 'Cruise' THEN 'Cruise Terminal'
		ELSE 'Port or Marine Terminal'
	END) as factype,
    'Ports and Ferry Landings' as facsubgrp,
    (CASE
		WHEN operators like '%Sanitation%' THEN 'NYC Department of Sanitation'
		WHEN operators like '%Department of Environmental Protection%' THEN 'NYC Department of Environmental Protection'
        WHEN operators like '%Department of Transportation%' THEN 'NYC Department of Transportation'
        WHEN operators like '%Department of Ports and Terminals%' THEN 'NYC Department of Port and Terminals'
        WHEN operators like '%Department of Interior%' THEN 'US Department of Interior'
        WHEN operators like '%Police%' THEN 'NYC Police Department'
        WHEN operators like '%Fire Department%' THEN 'NYC Fire Department'
        WHEN operators like '%Corrections%' THEN 'NYC Department of Correction'
        WHEN operators like '%State University%' THEN 'State University of New York'
        WHEN operators like '%Coast Guard%' THEN 'US Coast Guard'
        WHEN operators IS NULL AND owners like '%Port Authority%' THEN 'Port Authority of New York and New Jersey'
        WHEN operators IS NULL AND owners like '%Parks%' THEN 'NYC Department of Parks and Recreation'
        WHEN operators IS NULL AND owners like '%Department of Environmental Protection%' THEN 'NYC Department of Environmental Protection'
        WHEN operators IS NULL AND owners like '%Department of Sanitation%' THEN 'NYC Department of Sanitation'
        WHEN operators IS NULL AND owners like '%Department of Transportation%' THEN 'NYC Department of Transportation'
        WHEN operators IS NULL AND owners like '%Department of Port and Terminals%' THEN 'NYC Department of Port and Terminals'
        WHEN operators IS NULL AND owners like '%Department of Interior%' THEN 'US Department of Interior'
        WHEN operators IS NULL AND owners like '%Police%' THEN 'NYC Police Department'
        WHEN operators IS NULL AND owners like '%Fire Department%' THEN 'NYC Fire Department'
        WHEN operators IS NULL AND owners like '%Corrections%' THEN 'NYC Department of Correction'
        WHEN operators IS NULL AND owners like '%State University%' THEN 'State University of New York'
        WHEN operators IS NULL AND owners like '%Coast Guard%' THEN 'US Coast Guard'
        ELSE 'Non-public'
    END) as opname,
    (CASE
        WHEN operators like '%Sanitation%' THEN 'NYCDSNY'
        WHEN operators like '%Department of Environmental Protection%' THEN 'NYCDEP'
        WHEN operators like '%Department of Transportation%' THEN 'NYCDOT'
        WHEN operators like '%Department of Ports and Terminals%' THEN 'NYCDPT'
        WHEN operators like '%Department of Interior%' THEN 'USDOI'
        WHEN operators like '%Police%' THEN 'NYCNYPD'
        WHEN operators like '%Fire Department%' THEN 'NYCFDNY'
        WHEN operators like '%Corrections%' THEN 'NYCDOC'
        WHEN operators like '%State University%' THEN 'SUNY'
        WHEN operators like '%Coast Guard%' THEN 'USCG'
        WHEN operators IS NULL AND owners like '%Port Authority%' THEN 'PANYNJ'
        WHEN operators IS NULL AND owners like '%Economic Development%' THEN 'NYCEDC'
        WHEN operators IS NULL AND owners like '%Parks%' THEN 'NYCDPR'
        WHEN operators IS NULL AND owners like '%Department of Environmental Protection%' THEN 'NYCDEP'
        WHEN operators IS NULL AND owners like '%Sanitation%' THEN 'NYCDSNY'
        WHEN operators IS NULL AND owners like '%Transportation%' THEN 'NYCDOT'
        WHEN operators IS NULL AND owners like '%Department of Port and Terminals%' THEN 'NYCDPT'
        WHEN operators IS NULL AND owners like '%Department of Interior%' THEN 'USDOI'
        WHEN operators IS NULL AND owners like '%Police%' THEN 'NYCNYPD'
        WHEN operators IS NULL AND owners like '%Fire Department%' THEN 'NYCFDNY'
        WHEN operators IS NULL AND owners like '%Corrections%' THEN 'NYCDOC'
        WHEN operators IS NULL AND owners like '%State University%' THEN 'SUNY'
        WHEN operators IS NULL AND owners like '%Coast Guard%' THEN 'USCG'
        ELSE 'Non-public'
    END) as opabbrev,
    (CASE
        WHEN owners like '%Port Authority%' THEN 'PANYNJ'
        WHEN owners like '%Economic Development%' THEN 'NYCEDC'
        WHEN owners like '%Parks%' THEN 'NYCDPR'
        WHEN owners like '%Department of Environmental Protection%' THEN 'NYCDEP'
        WHEN owners like '%Department of Transportation%' THEN 'NYCDOT'
        WHEN owners like '%Department of Port and Terminals%' THEN 'NYCDPT'
        WHEN owners like '%Department of Interior%' THEN 'USDOI'
        WHEN owners like '%Police%' THEN 'NYCNYPD'
        WHEN owners like '%Fire Department%' THEN 'NYCFDNY'
        WHEN owners like '%Corrections%' THEN 'NYCDOC'
        WHEN owners like '%State University%' THEN 'SUNY'
        WHEN owners like '%Coast Guard%' THEN 'USCG'
        WHEN owners like '%United States%' THEN 'USCG'
        WHEN owners = 'Current Owner: City of New York.' THEN 'NYC-Unknown'
        WHEN operators like '%Sanitation%' THEN 'NYCDSNY'
        WHEN operators like '%Department of Environmental Protection%' THEN 'NYCDEP'
        WHEN operators like '%Department of Sanitation%' THEN 'NYCDOT'
        WHEN operators like '%Department of Ports and Terminals%' THEN 'NYCDPT'
        WHEN operators like '%Department of Interior%' THEN 'USDOI'
        WHEN operators like '%Police%' THEN 'NYCNYPD'
        WHEN operators like '%Fire Department%' THEN 'NYCFDNY'
        WHEN operators like '%Corrections%' THEN 'NYCDOC'
        WHEN operators like '%State University%' THEN 'SUNY'
        WHEN operators like '%Coast Guard%' THEN 'USCG'
        ELSE 'Non-public'
    END) as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    NULL geo_1b,
    NULL geo_bl,
    NULL geo_bn
INTO _usdot_ports
FROM usdot_ports
WHERE nav_unit_n ~* '^port|terminal|ferry|cruise';

CALL append_to_facdb_base('_usdot_ports');
