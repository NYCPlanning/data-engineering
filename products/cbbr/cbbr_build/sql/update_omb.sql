-- adding new fields for OMB data
ALTER TABLE cbbr_submissions
ADD newtrackingno text,
ADD requestcategory text,
ADD needcode text,
ADD projectid1 text,
ADD projectid2 text,
ADD projectid3 text,
ADD budgetline1 text,
ADD budgetline2 text,
ADD budgetline3 text,
ADD agyresponsecode text,
ADD additionalcomment text;

-- updating cbbr with omb data
UPDATE cbbr_submissions a
-- new fields
SET
    newtrackingno = b.newtrackingno,
    requestcategory = b.requestcategory,
    needcode = b.needcode,
    projectid1 = b.projectid1,
    projectid2 = b.projectid2,
    projectid3 = b.projectid3,
    budgetline1 = b.budgetline1,
    budgetline2 = b.budgetline2,
    budgetline3 = b.budgetline3,
    agyresponsecode = b.agyresponsecode,
    additionalcomment = b.additionalcomment,
    -- existing fields
    agency = b.agency,
    agyresponse = b.agyresponse,
    agyresponsecat = b.agyresponsecat || ' - ' || b.agyresponsecatdesc
FROM (
    SELECT
        a.*,
        regid
    FROM cbbr_ombresponse AS a
    LEFT JOIN cbbr_omblookuptable AS b
        ON a.newtrackingno = b.newtrackingno
) AS b
WHERE upper(a.regid) = upper(b.regid);

DELETE FROM cbbr_submissions
WHERE regid IN (
    SELECT a.regid FROM cbbr_submissions AS a
    LEFT JOIN (
        SELECT
            a.*,
            regid AS regidb
        FROM cbbr_ombresponse AS a
        LEFT JOIN cbbr_omblookuptable AS b
            ON a.newtrackingno = b.newtrackingno
    ) AS b
        ON upper(a.regid) = b.regidb
    WHERE b.regidb IS NULL
);

-- dropping tables
DROP TABLE IF EXISTS cbbr_ombresponse;
DROP TABLE IF EXISTS cbbr_omblookuptable;
