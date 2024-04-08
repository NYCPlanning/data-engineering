/** QAQC
	JOB_TYPE:
		dem_nb_overlap
    UNITS:
        units_init_null
	    units_init_null
        dup_equal_units -> dup_bbl_address_units
        dup_diff_units -> dup_bbl_address
    OCC:
        b_nonres_with_units
	    units_res_accessory
	    b_likely_occ_desc
    CO:
        units_co_prop_mismatch
    STATUS:
        incomp_tract_home
**/

DROP TABLE IF EXISTS match_dem_nb;
SELECT
    a.job_number AS job_number_dem,
    b.job_number AS job_number_nb,
    a.geo_bbl
INTO match_dem_nb
FROM mid_devdb AS a
INNER JOIN mid_devdb AS b
    ON a.geo_bbl = b.geo_bbl
WHERE
    a.job_type = 'Demolition'
    AND b.job_type = 'New Building';

-- JOBNUMBER_duplicates
DROP TABLE IF EXISTS jobnumber_duplicates;
WITH
bbl_address_groups AS (
    SELECT
        coalesce(geo_bbl, 'NULL BBL') AS geo_bbl,
        address
    FROM mid_devdb
    WHERE
        job_inactive IS NULL
        AND address IS NOT NULL
    GROUP BY coalesce(geo_bbl, 'NULL BBL'), address
    HAVING count(*) > 1
),
bbl_address_unit_groups AS (
    SELECT
        coalesce(geo_bbl, 'NULL BBL') AS geo_bbl,
        address,
        classa_net
    FROM mid_devdb
    WHERE
        job_inactive IS NULL
        AND address IS NOT NULL
        AND classa_net IS NOT NULL
    GROUP BY coalesce(geo_bbl, 'NULL BBL'), address, classa_net
    HAVING count(*) > 1
)
SELECT
    job_number,
    CASE
        WHEN
            geo_bbl || address || classa_net
            IN (
                SELECT geo_bbl || address || classa_net
                FROM bbl_address_unit_groups
            )
            THEN coalesce(geo_bbl, 'NULL BBL') || ' : ' || address || ' : ' || classa_net
    END AS dup_bbl_address_units,
    CASE
        WHEN
            geo_bbl || address
            IN (
                SELECT geo_bbl || address
                FROM bbl_address_groups
            )
            THEN coalesce(geo_bbl, 'NULL BBL') || ' : ' || address
    END AS dup_bbl_address
INTO jobnumber_duplicates
FROM mid_devdb;

DROP TABLE IF EXISTS mid_qaqc;
WITH
jobnumber_dem_nb AS (
    SELECT DISTINCT job_number_dem AS job_number FROM match_dem_nb
    UNION
    SELECT DISTINCT job_number_nb AS job_number FROM match_dem_nb
),
jobnumber_co_prop_mismatch AS (
    SELECT
        job_number,
        co_latest_certtype
    FROM mid_devdb
    WHERE
        job_type ~* 'New Building|Alteration'
        AND co_latest_units != classa_prop
),
jobnumber_incomplete_tract AS (
    SELECT job_number
    FROM mid_devdb
    WHERE
        tracthomes = 'Y'
        AND job_status ~* '1|2|3'
)
SELECT DISTINCT
    status_qaqc.*,
    (
        job_type IN ('Demolition', 'Alteration')
        AND resid_flag = 'Residential'
        AND classa_init IS NULL
    )::integer AS units_init_null,
    (
        job_type IN ('New Building', 'Alteration')
        AND resid_flag = 'Residential'
        AND classa_prop IS NULL
    )::integer AS units_prop_null,
    (
        (occ_initial !~* 'residential' AND classa_init != 0 AND classa_init IS NOT NULL)
        OR (occ_proposed !~* 'residential' AND classa_prop != 0 AND classa_prop IS NOT NULL)
    )::integer AS b_nonres_with_units,
    (
        (
            (
                address_numbr LIKE '%GAR%'
                OR job_desc ~* 'pool|shed|gazebo|garage'
            ) AND (
                classa_init::numeric IN (1, 2)
                OR classa_prop::numeric IN (1, 2)
            )
        )
        OR (
            (
                occ_initial LIKE '%(U)%'
                OR occ_initial LIKE '%(K)%'
                OR occ_proposed LIKE '%(U)%'
                OR occ_proposed LIKE '%(K)%'
            )
            AND (
                classa_init::numeric > 0
                OR classa_prop::numeric > 0
            )
        )
    )::integer AS units_res_accessory,
    (
        occ_initial ~* 'hotel|assisted|incapacitated|restrained'
        OR occ_proposed ~* 'hotel|assisted|incapacitated|restrained'
        OR job_desc ~* concat(
            'Hotel|Motel|Boarding|Hostel|Lodge|UG 5|UG5', '|',
            'Group 5|Grp 5|Class B|Class ''b''|Class "b"', '|',
            'SRO |Single room|Furnished|Rooming unit', '|',
            'Dorm |Dorms |Dormitor|Transient|Homeless', '|',
            'Shelter|Group quarter|Beds|Convent|Monastery', '|',
            'Accommodation|Harassment|CNH|Settlement|Halfway', '|',
            'Nursing home|Assisted|Supportive|Sleeping', '|',
            'UG3|UG 3|Group 3|Grp 3'
        )
    )::integer AS b_likely_occ_desc,
    (
        CASE
            WHEN a.job_number IN (SELECT job_number FROM jobnumber_co_prop_mismatch)
                THEN c.co_latest_certtype
        END
    ) AS units_co_prop_mismatch,
    (
        tracthomes = 'Y'
        AND job_status ~* '1|2|3'
    )::integer AS incomp_tract_home,
    (a.job_number IN (SELECT job_number FROM jobnumber_dem_nb))::integer AS dem_nb_overlap,
    b.dup_bbl_address,
    b.dup_bbl_address_units
INTO mid_qaqc
FROM mid_devdb AS a
LEFT JOIN status_qaqc ON a.job_number = status_qaqc.job_number
LEFT JOIN jobnumber_duplicates AS b ON a.job_number = b.job_number
LEFT JOIN jobnumber_co_prop_mismatch AS c ON a.job_number = c.job_number;
