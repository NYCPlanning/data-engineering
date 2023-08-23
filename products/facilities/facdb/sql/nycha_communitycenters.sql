DROP TABLE IF EXISTS _nycha_communitycenters;

SELECT
    uid,
    source,
    development as facname,
    parsed_hnum as addressnum,
    parsed_sname as streetname,
    address as address,
    NULL as city,
    NULL as zipcode,
    borough as boro,
    NULL as borocode,
    bin,
    bbl,
    (CASE
      WHEN program_type = 'NORC' THEN 'NORC Services'
      WHEN program_type IS NULL THEN 'NYCHA Community Center'
      ELSE 'NYCHA Community Center - '|| initcap(program_type)
    END) as factype,
    (CASE
      WHEN program_type LIKE '%/%/%' THEN 'COMMUNITY CENTERS AND COMMUNITY PROGRAMS'
      WHEN program_type ~* 'Case Management' THEN 'LEGAL AND INTERVENTION SERVICES'
      WHEN program_type ~* 'UPK' THEN 'DOE UNIVERSAL PRE-KINDERGARTEN'
      WHEN program_type ~* 'Senior|NORC' THEN 'SENIOR SERVICES'
      WHEN program_type ~* 'Day Care|Child Care' THEN 'DAY CARE'
      WHEN program_type ~* 'Disabilities' THEN 'PROGRAMS FOR PEOPLE WITH DISABILITIES'
      WHEN program_type ~* 'Storage' THEN 'STORAGE'
      WHEN program_type ~* 'Job Readiness|Jobs Plus' THEN 'WORKFORCE DEVELOPMENT'
      WHEN program_type ~* 'Vocational|Trade School' THEN 'GED AND ALTERNATIVE HIGH SCHOOL EQUIVALENCY'
      WHEN program_type ~* 'School' THEN 'PUBLIC K-12 SCHOOLS'
      WHEN program_type ~* 'NYPD' THEN 'POLICE SERVICES'
      WHEN program_type ~* 'Food Pantry' THEN 'SOUP KITCHENS AND FOOD PANTRIES'
      WHEN program_type ~* 'Mental|Counseling' THEN 'MENTAL HEALTH'
      WHEN program_type ~* 'Clinic' THEN 'HOSPITALS AND CLINICS'
      WHEN program_type ~* 'Plasterer''s Shop' THEN 'CUSTODIAL'
      WHEN program_type ~* 'ESL|Literacy' THEN 'ADULT AND IMMIGRANT LITERACY'
      WHEN program_type ~* 'Training' THEN 'TRAINING AND TESTING'
      WHEN program_type ~* 'Library' THEN 'PUBLIC LIBRARIES'
      WHEN program_type ~* 'CCTV' THEN 'TELECOMMUNICATIONS'
      WHEN program_type ~* 'Office|Staff|Contractor|ORRR' THEN 'CITY GOVERNMENT OFFICES'
      WHEN program_type ~* 'RESERVED - OPERATIONS|TA USE|lighting vendor|Unknown|Vacant' THEN 'MISCELLANEOUS USE'
      WHEN program_type ~* 'Urban Family Center|Shelter' THEN 'NON-RESIDENTIAL HOUSING AND HOMELESS SERVICES'
      WHEN program_type ~* 'Head Start' THEN 'HEAD START'
      WHEN program_type ~* 'Child welfare|Family' THEN 'FINANCIAL ASSISTANCE AND SOCIAL SERVICES'
      ELSE 'COMMUNITY CENTERS AND COMMUNITY PROGRAMS'
    END) as facsubgrp,
    'NYC Housing Authority' as opname,
    'NYCHA' as opabbrev,
    'NYCHA' as overabbrev,
    NULL as capacity,
    NULL as captype,
    wkt::geometry as wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _nycha_communitycenters
FROM nycha_communitycenters;

CALL append_to_facdb_base('_nycha_communitycenters');
