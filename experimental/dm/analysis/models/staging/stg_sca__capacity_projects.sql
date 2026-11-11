SELECT
    name AS project_name,
    "Address" AS address,
    "Borough" AS borough,
    -- the file writes a missing level as <Null>
    nullif(org_level, '<Null>') AS org_level,
    "Capacity"::INTEGER AS seats,
    strptime(start_date, '%-m/%-d/%Y')::DATE AS start_date,
    "Capital Pl" AS capital_plan,
    -- Distzone is like 10 / 2, once 31 /4, and twice with a trailing space
    regexp_replace(trim("Distzone"), '\s*/\s*', '-') AS school_subdistrict_id,
    ST_GeomFromHEXWKB(geom) AS geom
FROM {{ source('sources', 'sca_capacity_projects') }}
