-- update geoms where names are the exact same
WITH master AS (
      WITH singlename AS (
            WITH filtered AS (
                  SELECT
                        facname,
                        COUNT(facname) AS namecount
                  FROM
                        _dcp_facilities
                  GROUP BY
                        facname
)
                  SELECT
                        facname
                  FROM
                        filtered
                  WHERE
                        namecount = 1
)
                  SELECT
                        a.unique_id,
                        a.facility_or_park_name,
                        b.facname,
                        b.geom
                  FROM
                        _cbbr_submissions a,
                        _dcp_facilities b
                  WHERE
                        a.geom IS NULL
                        AND a.facility_or_park_name IS NOT NULL
                        AND b.facname LIKE '%' || ' ' || '%' || ' ' || '%'
                        AND '%' || upper(a.facility_or_park_name) || '%' = '%' || upper(b.facname) || '%'
                        AND b.facname IN (
                              SELECT
                                    facname
                              FROM
                                    singlename)
                              AND upper(a.borough) = upper(b.boro))
            UPDATE
                  _cbbr_submissions
            SET
                  geo_function = '_dcp_facilities',
                  geom = master.geom
            FROM
                  master
            WHERE
                  _cbbr_submissions.unique_id = master.unique_id
                  AND _cbbr_submissions.geom IS NULL;

-- update lib geoms using facdb
WITH master AS (
      SELECT
            a.unique_id,
            a.facility_or_park_name,
            b.facname,
            b.geom
      FROM
            _cbbr_submissions a,
            (
                  SELECT
                        *
                  FROM
                        _dcp_facilities
                  WHERE
                        upper(facgroup) = 'LIBRARIES') b
            WHERE (upper(a.facility_or_park_name)
                  LIKE '%LIBRARY%'
                  OR upper(a.facility_or_park_name)
                  LIKE '%BRANCH%')
            AND upper(a.facility_or_park_name)
            NOT LIKE '%AND%'
            AND upper(a.facility_or_park_name)
            NOT LIKE '%&%'
            AND (('%' || upper(replace(a.facility_or_park_name, ' Branch', ' Library')) || '%' LIKE '%' || upper(b.facname) || '%')
                  OR ('%' || upper(replace(a.facility_or_park_name, ' Branch', '')) || '%' LIKE '%' || upper(b.facname) || '%')
                  OR ('%' || upper(a.facility_or_park_name) || '%' LIKE '%' || upper(b.facname) || '%'))
            AND upper(a.borough) = upper(b.boro)
            AND a.geom IS NULL)
UPDATE
      _cbbr_submissions
SET
      geo_function = '_dcp_facilities',
      geom = master.geom
FROM
      master
WHERE
      _cbbr_submissions.unique_id = master.unique_id
      AND _cbbr_submissions.geom IS NULL;

-- update precinct geoms using facdb
WITH master AS (
      SELECT
            a.unique_id,
            a.facility_or_park_name,
            b.facname,
            b.geom
      FROM
            _cbbr_submissions a,
            (
                  SELECT
                        *
                  FROM
                        _dcp_facilities
                  WHERE
                        upper(facsubgrp) = 'POLICE SERVICES') b
            WHERE (upper(a.facility_or_park_name)
                  LIKE '%PRECINCT%'
                  OR upper(a.facility_or_park_name)
                  LIKE '%PCT%')
            AND upper(a.facility_or_park_name)
            NOT LIKE '%AND%'
            AND regexp_replace(a.facility_or_park_name, '\D', '', 'g') = regexp_replace(b.facname,'\D', '', 'g')
            AND regexp_replace(a.facility_or_park_name, '\D', '', 'g') IS NOT NULL
            AND upper(a.borough) = upper(b.boro)
            AND a.geom IS NULL)
UPDATE
      _cbbr_submissions
SET
      geo_function = '_dcp_facilities',
      geom = master.geom
FROM
      master
WHERE
      _cbbr_submissions.unique_id = master.unique_id
      AND _cbbr_submissions.geom IS NULL;

-- update school geoms using facdb
WITH master AS (
      SELECT
            a.unique_id,
            a.facility_or_park_name,
            b.facname,
            b.geom
      FROM
            _cbbr_submissions a,
            (
                  SELECT
                        *
                  FROM
                        _dcp_facilities
                  WHERE
                        upper(facsubgrp) = 'PUBLIC K-12 SCHOOLS') b
            WHERE (upper(a.facility_or_park_name)
                  LIKE '%SCHOOL%'
                  OR upper(a.facility_or_park_name)
                  LIKE '%P.S.%'
                  OR upper(a.facility_or_park_name)
                  LIKE '%I.S.%'
                  OR upper(a.facility_or_park_name)
                  LIKE '%M.S.%'
                  OR upper(a.facility_or_park_name)
                  LIKE '%H.S.%'
                  OR upper(a.facility_or_park_name)
                  LIKE 'PS%'
                  OR upper(a.facility_or_park_name)
                  LIKE 'P.S%')
            AND upper(a.facility_or_park_name)
            NOT LIKE '%AND%'
            AND regexp_replace(a.facility_or_park_name, '\D', '', 'g') = regexp_replace(b.facname,'\D', '', 'g')
            AND regexp_replace(a.facility_or_park_name, '\D', '', 'g') <> ''
                  AND upper(a.borough) = upper(b.boro)
                  AND a.geom IS NULL)
            UPDATE
                  _cbbr_submissions
            SET
                  geo_function = '_dcp_facilities',
                  geom = master.geom
            FROM
                  master
            WHERE
                  _cbbr_submissions.unique_id = master.unique_id
                  AND _cbbr_submissions.geom IS NULL;

-- update school geoms using facdb
-- round 2 after removing leading 0s
WITH master AS (
      SELECT
            a.unique_id,
            a.facility_or_park_name,
            b.facname,
            b.geom
      FROM
            _cbbr_submissions a,
            (
                  SELECT
                        *
                  FROM
                        _dcp_facilities
                  WHERE
                        upper(facsubgrp) = 'PUBLIC K-12 SCHOOLS') b
            WHERE (upper(a.facility_or_park_name)
                  LIKE '%SCHOOL%'
                  OR upper(a.facility_or_park_name)
                  LIKE '%P.S.%'
                  OR upper(a.facility_or_park_name)
                  LIKE '%I.S.%'
                  OR upper(a.facility_or_park_name)
                  LIKE '%M.S.%'
                  OR upper(a.facility_or_park_name)
                  LIKE '%H.S.%'
                  OR upper(a.facility_or_park_name)
                  LIKE 'PS%'
                  OR upper(a.facility_or_park_name)
                  LIKE 'P.S%')
            AND upper(a.facility_or_park_name)
            NOT LIKE '%AND%'
            AND regexp_replace(a.facility_or_park_name, '\D', '', 'g') = regexp_replace(replace(replace(b.facname,' 0', ' '),' 0', ' '),'\D', '', 'g')
            AND regexp_replace(a.facility_or_park_name, '\D', '', 'g') <> ''
                  AND upper(a.borough) = upper(b.boro)
                  AND a.geom IS NULL)
            UPDATE
                  _cbbr_submissions
            SET
                  geo_function = '_dcp_facilities',
                  geom = master.geom
            FROM
                  master
            WHERE
                  _cbbr_submissions.unique_id = master.unique_id
                  AND _cbbr_submissions.geom IS NULL;

-- update park geoms using facdb
WITH master AS (
      SELECT
            a.unique_id,
            a.facility_or_park_name,
            b.facname,
            a.agency_acronym,
            a.borough,
            b.geom
      FROM
            _cbbr_submissions a,
            (
                  SELECT
                        *
                  FROM
                        _dcp_facilities
                  WHERE
                        upper(facgroup) = 'PARKS AND PLAZAS') b
            WHERE (upper(a.facility_or_park_name)
                  LIKE '%PARK%'
                  OR upper(a.facility_or_park_name)
                  LIKE '%PLAYGROUND%'
                  OR agency_acronym = 'DPR')
            AND upper(a.facility_or_park_name)
            NOT LIKE '%AND%'
            AND (('%' || replace(upper(a.facility_or_park_name), ' PARK', '') || '%' LIKE '%' || replace(upper(b.facname), ' PARK', '') || '%')
                  OR ('%' || replace(upper(a.facility_or_park_name), ' PARK', '') || '%' LIKE '%' || replace(upper(b.facname), ' PLAYGROUND', '') || '%')
                  OR ('%' || replace(upper(a.facility_or_park_name), ' PLAYGROUND', '') || '%' LIKE '%' || replace(upper(b.facname), ' PLAYGROUND', '') || '%')
                  OR ('%' || replace(upper(a.facility_or_park_name), ' PLAYGROUND', '') || '%' LIKE '%' || replace(upper(b.facname), ' PARK', '') || '%'))
            AND upper(b.facname) <> 'PARK'
            AND upper(b.facname) <> 'LOT'
            AND upper(b.facname) <> 'GARDEN'
            AND upper(b.facname) <> 'TRIANGLE'
            AND upper(b.facname) <> 'SITTING AREA'
            AND upper(b.facname) <> 'BRIDGE PARK'
            AND upper(b.facname)
            NOT LIKE '%GARDEN%'
            AND upper(a.facility_or_park_name)
            NOT LIKE '%GARDEN%'
            AND upper(a.facility_or_park_name)
            NOT LIKE '%WOOD%'
            AND upper(a.borough) = upper(b.boro)
            AND a.geom IS NULL)
UPDATE
      _cbbr_submissions
SET
      geo_function = '_dcp_facilities',
      geom = master.geom
FROM
      master
WHERE
      _cbbr_submissions.unique_id = master.unique_id
      AND _cbbr_submissions.geom IS NULL;

