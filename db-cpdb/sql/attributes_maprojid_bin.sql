-- attribute maprojid --> bin map
DROP TABLE IF EXISTS attributes_maprojid_bin;
-- Add bins from ddc
-- then spatial join

-- create temporary table which may have duplicates
CREATE TABLE attributes_maprojid_bin_tmp AS (
  SELECT *
  FROM (
    SELECT maprojid,
           bin::text
    FROM dcp_id_bin_map
    UNION
    SELECT a.maprojid,
           b.bin::text
    FROM cpdb_dcpattributes a,
         dcp_facilities b
    WHERE a.magency::int in (39, 37, 38, 35) AND
          b.facgroup = 'Libraries' AND
          upper(a.description) NOT LIKE '%AND%' AND
          '%'||upper(a.description)||'%' LIKE '%'||upper(b.facname)||'%'
    UNION
    SELECT a.maprojid,
           b.bin::text
    FROM cpdb_dcpattributes a,
         dcp_facilities b
    WHERE a.magency in ('850', '801', '806', '126', '819', '57', '72', '858', '827', '71', '56', '816', '125', '998') AND
          b.facname LIKE '%'||' '||'%'||' '||'%' AND
          upper(a.description) LIKE '%' || upper(b.facname) || '%' AND
          b.facname in (SELECT facname
                        FROM (SELECT facname,
                                     COUNT(facname) as namecount
                              FROM dcp_facilities
                              GROUP BY facname) z
                         WHERE z.namecount = 1)   
  ) a
  ORDER BY maprojid
);

INSERT INTO attributes_maprojid_bin_tmp
SELECT b.maprojid,
       c.bin
FROM cpdb_dcpattributes b,
     doitt_buildingfootprints c 
WHERE ST_Within(b.geom, c.wkb_geometry) AND
      b.maprojid NOT IN ( SELECT maprojid from attributes_maprojid_bin_tmp) AND
      maprojid = b.maprojid;

-- create the table dropping duplicates
CREATE TABLE attributes_maprojid_bin AS (
SELECT DISTINCT *
FROM attributes_maprojid_bin_tmp);

DROP TABLE attributes_maprojid_bin_tmp;
