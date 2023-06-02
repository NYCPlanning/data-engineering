DROP TABLE IF EXISTS _doe_lcgms;
WITH latest_sca_data AS (
	SELECT *
	FROM sca_enrollment_capacity
	WHERE data_as_of::date =
		(SELECT
			MAX(data_as_of::date)
		FROM sca_enrollment_capacity)
)
SELECT md5(a.uid || coalesce(b.uid, '')) as uid,
	a.source,
	a.location_name as facname,
	a.parsed_hnum as addressnum,
	a.parsed_sname as streetname,
	a.primary_address as address,
	a.city,
	a.zip as zipcode,
	a.city as boro,
	NULL as borocode,
	NULL as bin,
	a.borough_block_lot as bbl,
	(
		CASE
			WHEN a.managed_by_name = 'Charter'
			AND a.location_category_description ~* '%school%' THEN CASE
				WHEN a.location_type_description NOT LIKE '%Special%' THEN CONCAT(a.location_category_description, ' - Charter')
				WHEN a.location_type_description LIKE '%Special%' THEN CONCAT(
					a.location_category_description,
					' - Charter, Special Education'
				)
			END
			WHEN a.managed_by_name = 'Charter' THEN CASE
				WHEN a.location_type_description NOT LIKE '%Special%' THEN CONCAT(
					trim(
						regexp_replace(
							a.location_category_description,
							'school|School',
							''
						)
					),
					' School - Charter'
				)
				WHEN a.location_type_description LIKE '%Special%' THEN CONCAT(
					a.location_category_description,
					' School - Charter, Special Education'
				)
			END
			WHEN a.location_category_description ~* '%school%' THEN CASE
				WHEN a.location_type_description NOT LIKE '%Special%' THEN CONCAT(a.location_category_description, ' - Public')
				WHEN a.location_type_description LIKE '%Special%' THEN CONCAT(
					a.location_category_description,
					' - Public, Special Education'
				)
			END
			WHEN a.location_type_description LIKE '%Special%' THEN CONCAT(
				trim(
					regexp_replace(
						a.location_category_description,
						'school|School',
						''
					)
				),
				' School - Public, Special Education'
			)
			ELSE CONCAT(
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
	) as factype,
	(
		CASE
			WHEN a.location_type_description LIKE '%Special%' THEN 'Public and Private Special Education Schools'
			WHEN a.location_category_description LIKE '%Early%'
			OR a.location_category_description LIKE '%Pre-K%' THEN 'DOE Universal Pre-Kindergarten'
			WHEN a.managed_by_name = 'Charter' THEN 'Charter K-12 Schools'
			ELSE 'Public K-12 Schools'
		END
	) as facsubgrp,
	(
		CASE
			WHEN a.managed_by_name = 'Charter' THEN a.location_name
			ELSE 'NYC Department of Education'
		END
	) as opname,
	(
		CASE
			WHEN a.managed_by_name = 'Charter' THEN 'Non-public'
			ELSE 'NYCDOE'
		END
	) as opabbrev,
	'NYCDOE' as overabbrev,
	b.target_capacity as capacity,
	'seats' as captype,
	NULL as wkb_geometry,
	a.geo_1b,
	a.geo_bl,
	NULL as geo_bn
INTO _doe_lcgms
FROM doe_lcgms a
	LEFT JOIN latest_sca_data b ON (
		(a.location_code || a.building_code) = (b.org_id || b.bldg_id)
	)
WHERE a.location_category_description NOT IN ('Early Childhood','District Pre-K Center')
;
CALL append_to_facdb_base('_doe_lcgms');
