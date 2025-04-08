-- noqa: disable=all
DROP TABLE IF EXISTS dcas_ipis_parcel_names;
WITH distinct_names AS (
    SELECT DISTINCT parcel_name
    FROM dcas_ipis
),
compare AS (
    SELECT
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                    REGEXP_REPLACE(
                                                        REGEXP_REPLACE(
                                                            REGEXP_REPLACE(
                                                                REGEXP_REPLACE(
                                                                    REGEXP_REPLACE(
                                                                        REGEXP_REPLACE(
                                                                            REGEXP_REPLACE(
                                                                                REGEXP_REPLACE(
                                                                                    REGEXP_REPLACE(
                                                                                        REGEXP_REPLACE(
                                                                                            REGEXP_REPLACE(
                                                                                                REGEXP_REPLACE(
                                                                                                    REGEXP_REPLACE(
                                                                                                        REGEXP_REPLACE(
                                                                                                            REGEXP_REPLACE(
                                                                                                                REGEXP_REPLACE(
                                                                                                                    REGEXP_REPLACE(' '||TRIM(parcel_name)||' ', 
                                                                                                                    '\s&\s|&', 
                                                                                                                    ' AND '
                                                                                                                    ), 
                                                                                                                'PLAYLOT\s|PLGD\s|PLAYGRD\s|PLGRD\s|PLAYGND\s|PLYGRND/|PLYGRND\s|PLYGD\s|PLYGD|PLAYGRND\s|PLYGRD\s|PLYGRD/|PLGRD/|PLGRD\s|PLGD.\s|PLGD\s|PLYGD/|PLYGD|PLYGD\s|PLAYGD\s|PLGND\s|PLGD/|PLGD-|PLAYGND-', 
                                                                                                                'PLAYGROUND '
                                                                                                                ),
                                                                                                            '\@',
                                                                                                            'AT'
                                                                                                            ),
                                                                                                        'HSES',
                                                                                                        'HOUSES'
                                                                                                        ),
                                                                                                    '\s\(GROUP 1\s',
                                                                                                    '\s(GROUP 1)\s'
                                                                                                    ),
                                                                                                '\sPK\s|\sPK-',
                                                                                                ' PARK '
                                                                                                ),
                                                                                            'STA\s|STA\.\s|STAION\s',
                                                                                            'STATION '
                                                                                            ),
                                                                                        'LIBR\s',
                                                                                        'LIBRARY '
                                                                                        ),
                                                                                    'INDUST\sPARK|IND\.\sPARK|INDUST''L PARK',
                                                                                    'INDUSTRIAL PARK'
                                                                                    ),
                                                                                'PCT\s|PCT\.\s',
                                                                                'PRECINCT '
                                                                                ),
                                                                            'N\.Y\.C\.P\.D\.|N\.Y\.P\.D\.|NYCPD',
                                                                            'NYPD'
                                                                            ),
                                                                        'F\.H\.A\.\.',
                                                                        'FHA '
                                                                        ),
                                                                    'GDN\s|GARD\s',
                                                                    'GARDEN '
                                                                    ),
                                                                'AVE\)|AVE\.\)',
                                                                'AVENUE)'
                                                                ),
                                                            '\(AVE|\(AVE\.',
                                                            '(AVENUE'
                                                            ),
                                                        '\sAVE\s|\sAVE\.\s|\sAVE-\s|\sAVE\.\s|\sAV\s',
                                                        ' AVENUE '
                                                        ),
                                                    'HUNTS%POINT%AVENUE%REHAB',
                                                    'HUNTS POINT AVENUE REHAB'
                                                    ),
                                                'AUTH\s',
                                                'AUTHORITY '
                                                ),
                                            'BKLN|BK|BKLYN',
                                            'BROOKLYN'
                                            ),
                                        '\sBX\s|\sBRNX\s|/BX\s|\sBX\.\s',
                                        ' BRONX '
                                        ),
                                    'QNS',
                                    'QUEENS'
                                    ),
                                '\sSI\s',
                                ' STATEN ISLAND '
                                ),
                            'MN\s|MAHN\s|MANH\s',
                            'MANHATTAN '
                            ),
                        'REHABS\(|REHAB\(',
                        'REHAB ('
                        ),
                    'REHABS|REHAS',
                    'REHAB'
                    ),
                'SNR CTR\s|SNR C\s|SENR CTR\s|SENIOR CR\s|SENIOR C\s|SENR CNTR\s|SENIOR CTR\s|SENIOR CNTR\s|SENIOR CENTR\s|SR CENTER\s',
                'SENIOR CENTER '
                ),
            'ATHL FIELD\s|ATHL. FIELD\s|ATH FIELD\s|ATH FLD\s|ATH. FIELD\s|ATHLETIC FL\s|ATHLTC FLD\s|ATHLTC FIELD\s|ALTHL FIELD\s',
            'ATHLETIC FIELD '
            )
        ) as new_name,
        parcel_name as old_name
    FROM distinct_names
)
SELECT *
INTO dcas_ipis_parcel_names
FROM compare;
