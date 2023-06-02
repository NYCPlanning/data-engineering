-- Apply updates to agency and acronym
UPDATE cbbr_submissions 
SET agency_acronym = upper(a.agencyacro)
FROM cbbr_agency_updates a 
WHERE cbbr_submissions.tracking_code = a.trkno;

-- Fix change to Queens Public Library
UPDATE cbbr_submissions 
SET agency_acronym = 'QPL'
WHERE cbbr_submissions.agency_acronym = 'QL';