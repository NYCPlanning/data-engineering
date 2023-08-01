-- create the agency field
ALTER TABLE _cbbr_submissions
    DROP COLUMN IF EXISTS agency_normalized;

ALTER TABLE _cbbr_submissions
    ADD COLUMN agency_normalized text;

-- setting the agency name
UPDATE
    _cbbr_submissions
SET
    agency_normalized = (
        CASE WHEN agency_acronym = 'ACS' THEN
            upper('Administration for Children''s Services')
        WHEN agency_acronym = 'BPL' THEN
            upper('Brooklyn Public Library')
        WHEN agency_acronym = 'CEOM' THEN
            upper('Citywide Event Coordination and Management')
        WHEN agency_acronym = 'DCA' THEN
            upper('Department of Consumer Affairs')
        WHEN agency_acronym = 'DCAS' THEN
            upper('Department of Citywide Administrative Services')
        WHEN agency_acronym = 'DCLA' THEN
            upper('Department of Cultural Affairs')
        WHEN agency_acronym = 'DCP' THEN
            upper('Department of City Planning')
        WHEN agency_acronym = 'DEP' THEN
            upper('Department of Environmental Protection')
        WHEN agency_acronym = 'DFTA' THEN
            upper('Department for the Aging')
        WHEN agency_acronym = 'DHS' THEN
            upper('Department of Homeless Services')
        WHEN agency_acronym = 'DHS, HRA' THEN
            upper('Department of Homeless Services / Human Resources Administration')
        WHEN agency_acronym = 'DOB' THEN
            upper('Department of Buildings')
        WHEN agency_acronym = 'DOE' THEN
            upper('Department of Education')
        WHEN agency_acronym = 'DOHMH' THEN
            upper('Department of Health and Mental Hygiene')
        WHEN agency_acronym = 'DoiTT' THEN
            upper('Dept of Information Technology & Telecommunications')
        WHEN agency_acronym = 'DOT' THEN
            upper('Department of Transportation')
        WHEN agency_acronym = 'DPR' THEN
            upper('Department of Parks and Recreation')
        WHEN agency_acronym = 'DSNY' THEN
            upper('Department of Sanitation')
        WHEN agency_acronym = 'DYCD' THEN
            upper('Department of Youth & Community Development')
        WHEN agency_acronym = 'EDC' THEN
            upper('Economic Development Corporation')
        WHEN agency_acronym = 'FDNY' THEN
            upper('Fire Department')
        WHEN agency_acronym = 'HHC' THEN
            upper('Health and Hospitals Corporation')
        WHEN agency_acronym = 'HPD' THEN
            upper('Department of Housing Preservation & Development')
        WHEN agency_acronym = 'HRA' THEN
            upper('Human Resources Administration')
        WHEN agency_acronym = 'LPC' THEN
            upper('Landmarks Preservation Commission')
        WHEN agency_acronym = 'MOCJ' THEN
            upper('Mayor''s Office of Criminal Justice')
        WHEN agency_acronym = 'MOME' THEN
            upper('Mayor''s Office of Media and Entertainment')
        WHEN agency_acronym = 'NYCHA' THEN
            upper('Housing Authority')
        WHEN agency_acronym = 'NYCTA' THEN
            upper('Transit Authority')
        WHEN agency_acronym = 'NYPD' THEN
            upper('Police Department')
        WHEN agency_acronym = 'NYPL' THEN
            upper('New York Public Library')
        WHEN agency_acronym = 'OEM' THEN
            upper('Office of Emergency Management')
        WHEN agency_acronym = 'OMB' THEN
            upper('Mayor''s Office of Management and Budget')
        WHEN agency_acronym = 'QPL' THEN
            upper('Queens Borough Public Library')
        WHEN agency_acronym = 'SBS' THEN
            upper('Department of Small Business Services')
        WHEN agency_acronym = 'SCA' THEN
            upper('School Construction Authority')
        WHEN agency_acronym = 'TLC' THEN
            upper('Taxi and Limousine Commission')
        ELSE
            NULL
        END);

