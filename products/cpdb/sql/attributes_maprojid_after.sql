DROP TABLE IF EXISTS cpdb_adminbounds;
CREATE TABLE cpdb_adminbounds AS (
SELECT * FROM attributes_maprojid_cd
UNION ALL
SELECT * FROM attributes_maprojid_censustracts
UNION ALL
SELECT * FROM attributes_maprojid_congressionaldistricts
UNION ALL
SELECT * FROM attributes_maprojid_councildistricts
UNION ALL
SELECT * FROM attributes_maprojid_firecompanies
UNION ALL
SELECT * FROM attributes_maprojid_municipalcourtdistricts
UNION ALL
SELECT * FROM attributes_maprojid_policeprecincts
UNION ALL
SELECT * FROM attributes_maprojid_schooldistricts
UNION ALL
SELECT * FROM attributes_maprojid_stateassemblydistricts
UNION ALL
SELECT * FROM attributes_maprojid_statesenatedistricts
UNION ALL
SELECT * FROM attributes_maprojid_trafficanalysiszones
);

DROP TABLE attributes_maprojid_cd;
DROP TABLE attributes_maprojid_censustracts;
DROP TABLE attributes_maprojid_congressionaldistricts;
DROP TABLE attributes_maprojid_councildistricts;
DROP TABLE attributes_maprojid_firecompanies;
DROP TABLE attributes_maprojid_municipalcourtdistricts;
DROP TABLE attributes_maprojid_policeprecincts;
DROP TABLE attributes_maprojid_schooldistricts; 
DROP TABLE attributes_maprojid_stateassemblydistricts;
DROP TABLE attributes_maprojid_statesenatedistricts;
DROP TABLE attributes_maprojid_trafficanalysiszones;