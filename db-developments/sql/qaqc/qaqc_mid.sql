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

DROP TABLE IF EXISTS MATCH_dem_nb;
SELECT 
	a.job_number as job_number_dem, 
	b.job_number as job_number_nb,
	a.geo_bbl
INTO MATCH_dem_nb
FROM MID_devdb a
JOIN MID_devdb b
ON a.geo_bbl = b.geo_bbl
WHERE a.job_type = 'Demolition'
AND b.job_type = 'New Building';

-- JOBNUMBER_duplicates
DROP TABLE IF EXISTS JOBNUMBER_duplicates;
WITH
BBL_ADDRESS_groups AS (
	SELECT COALESCE(geo_bbl, 'NULL BBL') as geo_bbl, address
	FROM MID_devdb
	WHERE job_inactive IS NULL
	AND address IS NOT NULL
	GROUP BY COALESCE(geo_bbl, 'NULL BBL'), address 
	HAVING COUNT(*) > 1
),
BBL_ADDRESS_UNIT_groups AS (
	SELECT COALESCE(geo_bbl, 'NULL BBL') as geo_bbl, address, classa_net
	FROM MID_devdb
	WHERE job_inactive IS NULL
	AND address IS NOT NULL
	AND classa_net IS NOT NULL
	GROUP BY COALESCE(geo_bbl, 'NULL BBL'), address, classa_net
	HAVING COUNT(*) > 1
)
SELECT 
	job_number,
	CASE WHEN geo_bbl||address||classa_net
		IN (SELECT geo_bbl||address||classa_net 
			FROM BBL_ADDRESS_UNIT_groups)
		THEN  COALESCE(geo_bbl, 'NULL BBL')||' : '||address||' : '||classa_net
		ELSE NULL
	END as dup_bbl_address_units,
	CASE WHEN geo_bbl||address
		IN (SELECT geo_bbl||address 
			FROM BBL_ADDRESS_groups)
		THEN COALESCE(geo_bbl, 'NULL BBL')||' : '||address
		ELSE NULL
	END as dup_bbl_address
INTO JOBNUMBER_duplicates
FROM MID_devdb a;

DROP TABLE IF EXISTS MID_qaqc;
WITH
JOBNUMBER_dem_nb AS (
	SELECT DISTINCT job_number_dem as job_number FROM MATCH_dem_nb UNION
	SELECT DISTINCT job_number_nb as job_number FROM MATCH_dem_nb
),
JOBNUMBER_co_prop_mismatch AS (
    SELECT job_number, co_latest_certtype
    FROM MID_devdb
    WHERE job_type ~* 'New Building|Alteration' 
    AND co_latest_units <> classa_prop
),
JOBNUMBER_incomplete_tract AS (
    SELECT job_number
    FROM MID_devdb
    WHERE tracthomes = 'Y'
    AND job_status ~* '1|2|3'
)
SELECT 
	DISTINCT STATUS_qaqc.*,
	(
		job_type IN ('Demolition' , 'Alteration') AND 
		resid_flag = 'Residential' AND 
		classa_init IS NULL
	)::integer as units_init_null,
	(
		job_type IN ('New Building' , 'Alteration') AND 
		resid_flag = 'Residential' AND 
		classa_prop IS NULL
	)::integer as units_prop_null,
	(
		(occ_initial !~* 'residential' AND classa_init <> 0 AND classa_init IS NOT NULL) OR 
		(occ_proposed !~* 'residential' AND classa_prop <> 0 AND classa_prop IS NOT NULL)
	)::integer as b_nonres_with_units,
	(
		(
			(
				address_numbr LIKE '%GAR%' OR 
				job_desc ~* 'pool|shed|gazebo|garage'
			) AND (
				classa_init::numeric IN (1,2) OR 
				classa_prop::numeric IN (1,2)
			)
		) OR 
		(
			(
				occ_initial LIKE '%(U)%' OR 
				occ_initial LIKE '%(K)%' OR 
				occ_proposed LIKE '%(U)%' OR 
				occ_proposed LIKE '%(K)%'
			) AND 
			(
				classa_init::numeric > 0 OR 
				classa_prop::numeric > 0
			)
		)
	)::integer as units_res_accessory,
	(
		occ_initial ~* 'hotel|assisted|incapacitated|restrained' OR 
		occ_proposed ~* 'hotel|assisted|incapacitated|restrained' OR 
		job_desc ~* CONCAT('Hotel|Motel|Boarding|Hostel|Lodge|UG 5|UG5', '|',
                          'Group 5|Grp 5|Class B|Class ''b''|Class "b"', '|',
                          'SRO |Single room|Furnished|Rooming unit', '|',
						  'Dorm |Dorms |Dormitor|Transient|Homeless', '|',
                          'Shelter|Group quarter|Beds|Convent|Monastery', '|',
                          'Accommodation|Harassment|CNH|Settlement|Halfway', '|',
                          'Nursing home|Assisted|Supportive|Sleeping', '|',
						  'UG3|UG 3|Group 3|Grp 3')
	)::integer as b_likely_occ_desc,
	(
		CASE WHEN a.job_number IN (SELECT job_number FROM JOBNUMBER_co_prop_mismatch) 
		THEN c.co_latest_certtype END
	) as units_co_prop_mismatch,
	(
		tracthomes = 'Y' AND 
		job_status ~* '1|2|3'
	)::integer as incomp_tract_home,
	(a.job_number IN (SELECT job_number from JOBNUMBER_dem_nb))::integer as dem_nb_overlap,
	b.dup_bbl_address, 
	b.dup_bbl_address_units
INTO MID_qaqc
FROM MID_devdb a
	LEFT JOIN STATUS_qaqc on a.job_number = STATUS_qaqc.job_number 
	LEFT JOIN JOBNUMBER_duplicates b ON a.job_number = b.job_number 
	LEFT JOIN JOBNUMBER_co_prop_mismatch c ON a.job_number = c.job_number;