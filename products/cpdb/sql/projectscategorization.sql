-- Categorizing projects based on keywords in the project description
-- Categories: 1) ITT, Vehicles, and Equipment 2) Lump Sum 3) Fixed Asset


--Group projects into the ITT, Vehicles, and Equipment category
--managing agency: ALL
--SELECT * FROM cpdb_dcpattributes
UPDATE cpdb_dcpattributes AS a
SET typecategory = p.typecategory
FROM type_category_patterns AS p
WHERE
    p.typecategory = 'ITT, Vehicles, and Equipment'
    AND upper(a.description) LIKE p.pattern
    AND (upper(a.description) NOT LIKE '%GARAGE%');


--Group projects into the Lump Sum category
--managing agency: ALL
--SELECT * FROM cpdb_dcpattributes
UPDATE cpdb_dcpattributes AS a
SET typecategory = p.typecategory
FROM type_category_patterns AS p
WHERE
    a.typecategory IS NULL
    AND p.typecategory = 'Lump Sum'
    AND upper(a.description) LIKE p.pattern
    AND (upper(description) NOT LIKE '%SPACE%')
    AND (upper(description) NOT LIKE '%RESTOR%');

--Group projects into the Fixed Asset category
--managing agency: ALL
UPDATE cpdb_dcpattributes AS a
SET typecategory = p.typecategory
FROM type_category_patterns AS p
WHERE
    a.typecategory IS NULL
    AND p.typecategory = 'Fixed Asset'
    AND upper(a.description) LIKE p.pattern;

--DPR specific
UPDATE cpdb_dcpattributes
SET typecategory = 'Fixed Asset'
WHERE
    description ~ '[BMQRX][0-9][0-9][0-9]' AND magencyacro = 'DPR'
    AND typecategory IS NULL;
