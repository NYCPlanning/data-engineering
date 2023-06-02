--- Get dcp_bblnumber and dcp_name from dcp_projectbbls
DROP TABLE IF EXISTS dcp_projectbbl_subset;
CREATE TABLE dcp_projectbbl_subset AS
SELECT 
     -- Extract the first 9 characters of the "dcp_name" column and rename it as "dcp_name" to match 
     SUBSTRING(dcp_name FROM 1 FOR 9) AS dcp_name,
     -- Rename dcp_bblnumber to bbl 
     dcp_bblnumber AS bbl
FROM dcp_projectbbls;

-- Get dcp_name/record_ids that have multiple bbls assoicated with them 
DROP TABLE IF EXISTS dcp_projectbbl_sca;
CREATE TABLE  dcp_projectbbl_sca AS (
  WITH counts AS (
    SELECT dcp_name, COUNT(*) 
    FROM (
      SELECT DISTINCT dcp_name, bbl 
      FROM dcp_projectbbl_subset
    ) a
    GROUP BY dcp_name
  )
  SELECT * 
  FROM counts 
  WHERE count > 1
);

-- Create zap_project_many_bbls table as used in previous iterations of the SCA aggregate scripts
DROP TABLE IF EXISTS zap_project_many_bbls;
-- Create the zap_projects table
CREATE TABLE zap_project_many_bbls (
     record_id text
);
-- Insert only distinct values from dcp_projectbbl_sca into zap_project_many_bbls table for sca aggregate tables
INSERT INTO zap_project_many_bbls (record_id)
SELECT DISTINCT dcp_name FROM dcp_projectbbl_sca;