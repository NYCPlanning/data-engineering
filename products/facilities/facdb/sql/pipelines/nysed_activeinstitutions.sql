ALTER TABLE nysed_nonpublicenrollment DROP COLUMN IF EXISTS uid,
DROP COLUMN IF EXISTS source;
DROP TABLE IF EXISTS _nysed_activeinstitutions;
WITH merged AS (
    SELECT
        nysed_activeinstitutions.*,
        -- TODO: remove cast once new version of nysed_npe archived with ingest
        -- nysed_nonpublicenrollment.*,
        prek::numeric,
        halfk::numeric,
        fullk::numeric,
        g01::numeric,
        g02::numeric,
        g03::numeric,
        g04::numeric,
        g05::numeric,
        g06::numeric,
        uge::numeric,
        g07::numeric,
        g08::numeric,
        g09::numeric,
        g10::numeric,
        g11::numeric,
        g12::numeric,
        ugs::numeric,
        (
            prek::numeric
            + halfk::numeric
            + fullk::numeric
            + g01::numeric
            + g02::numeric
            + g03::numeric
            + g04::numeric
            + g05::numeric
            + g06::numeric
            + uge::numeric
            + g07::numeric
            + g08::numeric
            + g09::numeric
            + g10::numeric
            + g11::numeric
            + g12::numeric
            + ugs::numeric
        ) AS enrollment,
        -- Pre-compute grade-band key for NON-PUBLIC SCHOOLS (factype depends on
        -- enrollment counts, not just source category values).
        -- All other pipelines use inst_type_description|inst_sub_type_description.
        CASE
            WHEN inst_type_description = 'NON-PUBLIC SCHOOLS'
                AND (
                    prek::numeric + halfk::numeric + fullk::numeric
                    + g01::numeric + g02::numeric + g03::numeric
                    + g04::numeric + g05::numeric + uge::numeric
                ) > 0
                THEN 'NON-PUBLIC SCHOOLS|ELEM'
            WHEN inst_type_description = 'NON-PUBLIC SCHOOLS'
                AND (g06::numeric + g07::numeric + g08::numeric) > 0
                THEN 'NON-PUBLIC SCHOOLS|MIDDLE'
            WHEN inst_type_description = 'NON-PUBLIC SCHOOLS'
                AND (g09::numeric + g10::numeric + g11::numeric + g12::numeric + ugs::numeric) > 0
                THEN 'NON-PUBLIC SCHOOLS|HIGH'
            WHEN inst_type_description = 'NON-PUBLIC SCHOOLS'
                THEN 'NON-PUBLIC SCHOOLS|OTHER'
            ELSE inst_type_description || '|' || inst_sub_type_description
        END AS classification_key
    FROM nysed_activeinstitutions
    LEFT JOIN nysed_nonpublicenrollment
        ON nysed_activeinstitutions.sed_code::bigint = nysed_nonpublicenrollment.beds_code
    WHERE (
        inst_type_description = 'PUBLIC SCHOOLS'
        AND inst_sub_type_description LIKE '%GED%'
    )
    OR inst_sub_type_description LIKE '%CHARTER SCHOOL%'
    OR (
        inst_type_description <> 'PUBLIC SCHOOLS'
        AND inst_type_description <> 'NON-IMF SCHOOLS'
        AND inst_type_description <> 'GOVERNMENT AGENCIES' -- MAY ACTUALLY WANT TO USE THESE
        AND inst_type_description <> 'INDEPENDENT ORGANIZATIONS'
        AND inst_type_description <> 'LIBRARY SYSTEMS'
        AND inst_type_description <> 'LOCAL GOVERNMENTS'
        AND inst_type_description <> 'SCHOOL DISTRICTS'
        AND inst_sub_type_description <> 'PUBLIC LIBRARIES'
        AND inst_sub_type_description <> 'HISTORICAL RECORDS REPOSITORIES'
        AND inst_sub_type_description <> 'CHARTER CORPORATION'
        AND inst_sub_type_description <> 'HOME BOUND'
        AND inst_sub_type_description <> 'HOME INSTRUCTED'
        AND inst_sub_type_description <> 'NYC BUREAU'
        AND inst_sub_type_description <> 'NYC NETWORK'
        AND inst_sub_type_description <> 'OUT OF DISTRICT PLACEMENT'
        AND inst_sub_type_description <> 'BUILDINGS UNDER CONSTRUCTION'
    )
)
SELECT
    uid,
    source,
    CASE
        WHEN inst_sub_type_description LIKE '%CHARTER SCHOOL%' THEN legal_name
        ELSE popular_name
    END AS facname, -- TODO - maybe just use legal for all, but for fix in 25v2 limiting scope
    NULL AS addressnum,
    NULL AS streetname,
    physical_address_line1 AS address,
    NULL AS city,
    NULL AS zipcode,
    county_description AS boro,
    NULL AS borocode,
    NULL AS bin,
    NULL AS bbl,
    lfs.factype,
    lft.facsubgrp,
    (
        CASE
            WHEN inst_type_description = 'PUBLIC SCHOOLS' THEN 'NYC Department of Education'
            WHEN inst_type_description LIKE '%NON-IMF%' THEN 'NYC Department of Education'
            WHEN inst_type_description = 'CUNY' THEN 'City University of New York'
            WHEN inst_type_description = 'SUNY' THEN 'State University of New York'
            ELSE initcap(legal_name)
        END
    ) AS opname,
    (
        CASE
            WHEN inst_type_description = 'PUBLIC SCHOOLS' THEN 'NYCDOE'
            WHEN inst_type_description = 'CUNY' THEN 'CUNY'
            WHEN inst_type_description = 'SUNY' THEN 'SUNY'
            ELSE 'Non-public'
        END
    ) AS opabbrev,
    'NYCDOE' AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    st_point(
        gis_longitute_x::double precision,
        gis_latitude_y::double precision
    ) AS wkb_geometry,
    geo_1b,
    NULL AS geo_bl,
    NULL AS geo_bn
INTO _nysed_activeinstitutions
FROM merged
LEFT JOIN lookup_factype_source AS lfs
    ON lfs.source_name = 'nysed_activeinstitutions'
    AND lfs.source_value = merged.classification_key
LEFT JOIN lookup_factype AS lft
    ON lft.factype = lfs.factype;

CALL append_to_facdb_base('_nysed_activeinstitutions');
