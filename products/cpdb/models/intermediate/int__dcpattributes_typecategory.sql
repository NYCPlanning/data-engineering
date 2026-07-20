SELECT
    ccpversion,
    maprojid,
    magency,
    magencyacro,
    projectid,
    description,
    CASE
        WHEN
            EXISTS (
                SELECT 1
                FROM {{ ref('type_category_patterns') }} AS p
                WHERE
                    p.typecategory = 'ITT, Vehicles, and Equipment'
                    AND UPPER(base.description) LIKE p.pattern
            )
            AND UPPER(base.description) NOT LIKE '%GARAGE%'
            THEN 'ITT, Vehicles, and Equipment'
        WHEN
            EXISTS (
                SELECT 1
                FROM {{ ref('type_category_patterns') }} AS p
                WHERE
                    p.typecategory = 'Lump Sum'
                    AND UPPER(base.description) LIKE p.pattern
            )
            AND UPPER(base.description) NOT LIKE '%SPACE%'
            AND UPPER(base.description) NOT LIKE '%RESTOR%'
            THEN 'Lump Sum'
        WHEN
            EXISTS (
                SELECT 1
                FROM {{ ref('type_category_patterns') }} AS p
                WHERE
                    p.typecategory = 'Fixed Asset'
                    AND UPPER(base.description) LIKE p.pattern
            )
            THEN 'Fixed Asset'
        WHEN
            base.description ~ '[BMQRX][0-9][0-9][0-9]'
            AND base.magencyacro = 'DPR'
            THEN 'Fixed Asset'
    END AS typecategory,
    geomsource,
    dataname,
    datasource,
    datadate,
    geom
FROM {{ ref('int__dcpattributes_base') }} AS base
