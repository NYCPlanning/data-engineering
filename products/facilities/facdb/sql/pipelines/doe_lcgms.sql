DROP TABLE IF EXISTS _doe_lcgms;
WITH latest_sca_data AS (
    SELECT *
    FROM sca_enrollment_capacity
    WHERE
        data_as_of::date
        = (
            SELECT max(s.data_as_of::date)
            FROM sca_enrollment_capacity AS s
        )
)
SELECT
    md5(a.uid || coalesce(b.uid, '')) AS uid,
    a.source,
    a.location_name AS facname,
    a.parsed_hnum AS addressnum,
    a.parsed_sname AS streetname,
    a.primary_address AS address,
    a.city,
    a.zip AS zipcode,
    a.city AS boro,
    NULL AS borocode,
    NULL AS bin,
    a.borough_block_lot AS bbl,
    (
        CASE
            WHEN
                a.managed_by_name = 'Charter'
                AND a.location_category_description ~* '%school%'
                THEN CASE
                    WHEN
                        a.location_type_description NOT LIKE '%Special%'
                        THEN concat(a.location_category_description, ' - Charter')
                    WHEN a.location_type_description LIKE '%Special%' THEN concat(
                        a.location_category_description,
                        ' - Charter, Special Education'
                    )
                END
            WHEN a.managed_by_name = 'Charter'
                THEN CASE
                    WHEN a.location_type_description NOT LIKE '%Special%'
                        THEN concat(
                            trim(
                                regexp_replace(
                                    a.location_category_description,
                                    'school|School',
                                    ''
                                )
                            ),
                            ' School - Charter'
                        )
                    WHEN a.location_type_description LIKE '%Special%' THEN concat(
                        a.location_category_description,
                        ' School - Charter, Special Education'
                    )
                END
            WHEN a.location_category_description ~* '%school%'
                THEN CASE
                    WHEN
                        a.location_type_description NOT LIKE '%Special%'
                        THEN concat(a.location_category_description, ' - Public')
                    WHEN a.location_type_description LIKE '%Special%' THEN concat(
                        a.location_category_description,
                        ' - Public, Special Education'
                    )
                END
            WHEN a.location_type_description LIKE '%Special%'
                THEN concat(
                    trim(
                        regexp_replace(
                            a.location_category_description,
                            'school|School',
                            ''
                        )
                    ),
                    ' School - Public, Special Education'
                )
            ELSE concat(
                trim(
                    regexp_replace(
                        a.location_category_description,
                        'school|School',
                        ''
                    )
                ),
                ' School - Public'
            )
        END
    ) AS factype,
    (
        CASE
            WHEN a.location_type_description LIKE '%Special%' THEN 'Public and Private Special Education Schools'
            WHEN
                a.location_category_description LIKE '%Early%'
                OR a.location_category_description LIKE '%Pre-K%' THEN 'DOE Universal Pre-Kindergarten'
            WHEN a.managed_by_name = 'Charter' THEN 'Charter K-12 Schools'
            ELSE 'Public K-12 Schools'
        END
    ) AS facsubgrp,
    (
        CASE
            WHEN a.managed_by_name = 'Charter' THEN a.location_name
            ELSE 'NYC Department of Education'
        END
    ) AS opname,
    (
        CASE
            WHEN a.managed_by_name = 'Charter' THEN 'Non-public'
            ELSE 'NYCDOE'
        END
    ) AS opabbrev,
    'NYCDOE' AS overabbrev,
    b.org_target_cap AS capacity,
    'seats' AS captype,
    NULL AS wkb_geometry,
    a.geo_1b,
    a.geo_bl,
    NULL AS geo_bn
INTO _doe_lcgms
FROM doe_lcgms AS a
LEFT JOIN latest_sca_data AS b
    ON (
        (a.location_code || a.building_code) = (b.org_id || b.bldg_id)
    )
WHERE a.location_category_description NOT IN ('Early Childhood', 'District Pre-K Center');
CALL append_to_facdb_base('_doe_lcgms');
