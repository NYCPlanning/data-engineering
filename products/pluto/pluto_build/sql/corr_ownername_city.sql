-- Take the owner name from the normalized list
-- Insert records into pluto_input_corrections
INSERT INTO pluto_changes_applied
SELECT DISTINCT a.bbl, 
	'ownername' as field, 
	a.ownername as old_value, 
	b.new_value as new_value,
	b.type as type,
	b.reason as reason,
	b.version as version
FROM pluto a, pluto_input_research b
WHERE a.ownername = b.old_value
	AND a.bbl NOT IN (SELECT bbl FROM pluto_changes_applied WHERE field = 'ownername');

INSERT INTO pluto_changes_not_applied
SELECT DISTINCT b.*
FROM pluto_input_research b, pluto a
WHERE b.bbl=a.bbl 
	AND b.field='ownername' 
	AND b.old_value <> a.ownername;

-- INSERT INTO pluto_corrections_applied
-- SELECT DISTINCT b.*
-- FROM pluto_corrections b, pluto a
-- WHERE b.bbl=a.bbl 
-- 	AND b.field='ownername' 
-- 	AND b.old_value = a.ownername;
	
-- Apply correction to PLUTO
UPDATE pluto a
SET ownername = b.new_value,
	dcpedited = 't'
FROM pluto_input_research b
WHERE a.ownername = b.old_value
AND b.field = 'ownername';

-- -- logic for creating normalzied value and old value lookup table
-- -- create temp table
-- DROP TABLE IF EXISTS pluto_corr_ownername_temp;
-- CREATE TABLE pluto_corr_ownername_temp AS (
-- SELECT bbl, ownername, ownername as newownername FROM pluto WHERE ownertype = 'C');
-- -- cerate normalized values
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'NEW YORK CITY', 'NYC');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'CITY OF NEW YORK', 'NYC');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'CITY OF NY', 'NYC');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'C N Y', 'NYC');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'NY STATE', 'NYS');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'DEPT', 'DEPARTMENT');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'DPT', 'DEPARTMENT');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'ACS', 'ADMINISTRATION FOR CHILDRENS SERVICES');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'DCAS', 'DEPARTMENT OF CITYWIDE ADMINISTRATIVE SERVICES');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'DDC', 'DEPARTMENT OF DESIGN AND CONSTRUCTION');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'DHS', 'DEPARTMENT OF HOMELESS SERVICES');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'DOITT', 'DEPARTMENT OF INFORMATION TECHNOLOGY AND TELECOMMUNICATIONS');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'DOT', 'DEPARTMENT OF TRANSPORTATION');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'DSBS', 'DEPARTMENT OF SMALL BUSINESS SERVICES');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'EDC', 'ECONOMIC DEVELOPMENT CORPORATION');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'HHC', 'HEALTH AND HOSPITALS CORPORATION');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'HPD', 'DEPARTMENT OF HOUSING PRESERVATION AND DEVELOPMENT');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'HRA', 'HUMAN RESOURCES ADMINISTRATION');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'OCME', 'OFFICE OF THE CHIEF MEDICAL EXAMINER');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'LIBRA', 'LIBRARY')
-- WHERE newownername LIKE '%LIBRA';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'SERVIC', 'SERVICES')
-- WHERE newownername LIKE '%SERVIC';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'DEPARTMENT.OF', 'DEPARTMENT OF')
-- WHERE newownername LIKE '%DEPARTMENT%OF%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'DEV ', 'DEVELOPMENT ')
-- WHERE newownername LIKE '% DEV %';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'CORP', 'CORPORATION')
-- WHERE newownername LIKE '%CORP';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'AUTH', 'AUTHORITY')
-- WHERE newownername LIKE '%AUTH';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'ACD', 'ADJOURMENTS IN CONTEMPLATION OF DISMISSAL')
-- WHERE newownername LIKE '%ACD';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,' & ', ' AND ');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'&', ' AND ');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC TRANSIT AUTHORITY'
-- WHERE newownername = 'NYCT'
-- 	OR newownername = 'NYCTA'
-- 	OR newownername LIKE 'NYC TRANSIT%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC DEPARTMENT OF PARKS AND RECREATION'
-- WHERE newownername LIKE '%PARKS AND RECREATION%'
-- 	OR newownername LIKE '%PARKS%DEPARTMENT%'
-- 	OR newownername LIKE '%DEPARTMENT%PARKS%'
-- 	OR newownername LIKE '%PARKS%ARSENAL%'
-- 	OR newownername LIKE 'NYC%PARKS%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC HOUSING PRESERVATION AND DEVELOPMENT'
-- WHERE newownername LIKE '%HOUS%PRES%DEVELOP%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC ECONOMIC DEVELOPMENT CORPORATION'
-- WHERE newownername LIKE '%NYC%ECONOMIC%DEVELOPMENT%'
-- 	OR newownername LIKE '%ECONOMIC%DEVELOPMENT%CORP%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC DEPARTMENT OF EDUCATION'
-- WHERE newownername LIKE 'NYC%EDUCATION%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'DEP', 'DEPARTMENT OF ENVIRONMENTAL PROTECTION')
-- WHERE newownername = 'DEP'
-- 	OR newownername = 'NYC DEP';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC DEPARTMENT OF ENVIRONMENTAL PROTECTION'
-- WHERE newownername LIKE 'NYC%ENVIRON%PROT%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC POLICE DEPARTMENT'
-- WHERE newownername LIKE 'NYPD%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC FIRE DEPARTMENT'
-- WHERE newownername LIKE 'FDNY%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC DEPARTMENT OF CULTURAL AFFAIRS'
-- WHERE newownername LIKE 'CULTURAL%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC DEPARTMENT OF SANITATION'
-- WHERE newownername LIKE '%SANITATION%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'PORT AUTHORITY OF NY AND NJ'
-- WHERE newownername LIKE '%PORT%AUTH%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC TAXI AND LIMOUSINE COMMISSION'
-- WHERE newownername LIKE '%TAXI%LIMO%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'MTA - STATEN ISLAND RAILWAY'
-- WHERE newownername LIKE '%MTA%-%STATEN%ISLAND%RAILWAY%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC OFFICE OF THE CHIEF MEDICAL EXAMINER'
-- WHERE newownername LIKE '%OFFICE%CHIEF%MEDICAL%';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'(', '');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,')', '');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,'.', '');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = replace(newownername,',', '');
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = concat('NYC ',newownername)
-- WHERE newownername LIKE 'DEPARTMENT%'
-- 	OR newownername LIKE 'ADMINISTRATION%'
-- 	OR newownername LIKE 'ECONOMIC%'
-- 	OR newownername LIKE 'FIRE%'
-- 	OR newownername LIKE 'HEALTH%'
-- 	OR newownername LIKE 'HOUSING P%'
-- 	OR newownername LIKE 'HUMAN%'
-- 	OR newownername LIKE 'POLICE%'
-- 	;
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'CITY OF NEW YORK'
-- WHERE newownername = 'NYC'
-- OR newownername = 'THE NYC';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC FIRE DEPARTMENT'
-- WHERE newownername = 'NYC FIRE DEPARTMENT                     250';
-- UPDATE pluto_corr_ownername_temp
-- SET newownername = 'NYC POLICE DEPARTMENT'
-- WHERE newownername = 'NYC POLICE DEPARTMENT                   1';

-- -- insert values into lookup table
-- INSERT INTO pluto_input_research
-- 	SELECT NULL as bbl,
-- 	'ownername' as field,
--   ownername as old_value,
--   newownername as new_value,
--   'city ageny normalized' as reason
-- FROM (SELECT DISTINCT ownername, newownername 
-- FROM pluto_corr_ownername_temp 
-- WHERE ownername <> newownername
-- AND (newownername LIKE 'CITY%' 
-- 	OR newownername LIKE 'MTA%' 
-- 	OR newownername LIKE 'NYC%' 
-- 	OR newownername LIKE 'PORT%' 
-- 	OR newownername LIKE 'NYS%' 
-- 	OR newownername LIKE 'NEW%')) b;

-- DROP TABLE pluto_corr_ownername_temp;