DROP TABLE IF EXISTS _moeo_socialservicesitelocations; -- noqa: disable=LT05

WITH tmp AS (
    SELECT min(uid) AS uid
    FROM moeo_socialservicesitelocations
    GROUP BY program_name || provider_name, address_1
)
SELECT
    uid,
    source,
    provider_name AS facname,
    parsed_hnum AS addressnum,
    parsed_sname AS streetname,
    address_1 AS address,
    city,
    postcode AS zipcode,
    borough AS boro,
    left(bin::text, 1) AS borocode,
    bin,
    bbl,
    (CASE
        WHEN program_name = 'NORC SITES' THEN 'NORC Services'
        WHEN program_name = 'TRANSPORTATION ONLY' THEN 'Transportation'
        WHEN program_name = 'School’s Out New York City (SONYC)' THEN 'School''s Out New York City (SONYC)'
        ELSE initcap(program_name)
    END) AS factype,
    (CASE
        WHEN
            program_name IN
            (
                'Adult Literacy',
                'Adult Literacy - Adult Basic Education/High School Equivalency',
                'Adult Literacy - English for Speakers of Other Languages'
            )
            THEN 'Adult and Immigrant Literacy'
        WHEN
            program_name IN
            (
                'Beacon',
                'COMPASS Elementary',
                'COMPASS Explore',
                'COMPASS High',
                'Cornerstone',
                'Educational Support: High School Youth',
                'PEAK Centers',
                'School’s Out New York City (SONYC)',
                'Teen Rapp',
                'Youth Recreational Services/Youth Athletic Leagues'
            )
            THEN 'After-School Programs'
        WHEN
            program_name IN
            (
                'Community Residential-OASAS',
                'Community Services-OASAS',
                'Medically Supervised Outpatient-OASAS',
                'Medically Supervised Outpatient-OASAS, Community Residential-OASAS, Outpatient Rehabilitation Services-OASAS',
                'Medically Supervised Outpatient-OASAS, Compulsive Gambling Education-OASAS',
                'Medically Supervised Outpatient-OASAS, Primary Prevention Services-OASAS',
                'Medically Supervised Outpatient-OASAS, Residential Services-OASAS',
                'Medically Supervised Outpatient-OASAS, Vocational Rehabilitation-OASAS, Medically Monitored Withdrawal-OASAS, Medically Supervised Withdrawal Services Outpatient-OASAS',
                'Medically Supervised Outpatient-OASAS, Vocational Rehabilitation-OASAS, Methadone Maintenance-OASAS',
                'Mental Hygiene Drop-In Centers',
                'Methadone Maintenance',
                'Methadone Maintenance-OASAS',
                'Other Prevention Services-OASAS, Medically Supervised Outpatient-OASAS, Primary Prevention Services-OASAS, Youth Clubhouse-OASAS',
                'Other Prevention Services-OASAS, Prevention Resource Center-OASAS, Primary Prevention Services-OASAS',
                'Primary Prevention Services-OASAS'
            )
            THEN 'Substance Use Disorder Treatment Programs'
        WHEN
            program_name IN
            ('Community Based Programs')
            THEN 'Community Centers and Community Programs'
        WHEN
            program_name IN
            (
                'Fatherhood',
                'Healthy Families',
                'Social Welfare'
            )
            THEN 'Financial Assistance and Social Services'
        WHEN
            program_name IN
            (
                'COVID19 Programs',
                'Customized Assistance Services (CAS)',
                'Intake Medical Services'
            )
            THEN 'Health Promotion and Disease Prevention'
        WHEN
            program_name IN
            (
                'Civics Classes',
                'Immigrant Family Services',
                'Immigrant Services'
            )
            THEN 'Immigrant Services'
        WHEN
            program_name IN
            (
                'AIM',
                'Alternative To Detention',
                'Alternative To Incarceration',
                'Anti-gun Violence Initiative',
                'Appellate Indigent Criminal Defense',
                'Arches',
                'Article 10 Petition Parental Representation',
                'Assigned Domestic Violence Counsel',
                'Child Advocacy Center',
                'Court Advocacy Services',
                'Court Based Programs',
                'Crime Victims Services',
                'Discharge and Reentry Services',
                'Emergency Intervention Services (Domestic Violence Shelters)',
                'Family Justice Center',
                'Hate Crimes Prevention',
                'ICM Plus',
                'Legal Services',
                'Mediation Services',
                'Next STEPS',
                'Parent Support Program',
                'Trial-Level Indigent Defense Representation',
                'Victim Services, Domestic Violence',
                'Victim Services, Other',
                'Young Adult Justice Program'
            )
            THEN 'Legal and Intervention Services'
        WHEN
            program_name IN
            (
                'Adolescent IMPACT',
                'Advocacy Services-OMH',
                'Advocacy Services-OMH, Psychosocial Club-OMH',
                'Advocacy Services-OMH, Supported SRO-OMH',
                'Advocacy Services-OMH, Vocational Services-OMH, Home Based Crisis Intervention-OMH, Non-Medicaid Care Coordination; (Non-Licensed Program)-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'Affirmative Business/Industry-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'Assertive Community Treatment-OMH',
                'Assisted Competitive Employment-OMH',
                'Assisted Competitive Employment-OMH, Advocacy Services-OMH',
                'Assisted Competitive Employment-OMH, Advocacy Services-OMH, Supported SRO-OMH',
                'Assisted Competitive Employment-OMH, Assertive Community Treatment-OMH',
                'Clinic Treatment-OMH',
                'Coordinated Children''s Services Initiative-OMH',
                'CPEP Crisis Intervention-OMH, CPEP Crisis Outreach-OMH',
                'CPEP Crisis Outreach-OMH',
                'Crisis Intervention-OMH',
                'Crisis Intervention-OMH, Non-Medicaid Care Coordination; (Non-Licensed Program)-OMH, Outreach-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'Crisis/Respite Beds-OMH',
                'Family Support Services-OMH',
                'Home Based Crisis Intervention-OMH',
                'Home Based Crisis Intervention-OMH, Advocacy Services-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'Home Based Crisis Intervention-OMH, Crisis Intervention-OMH',
                'Home Based Crisis Intervention-OMH, Crisis Intervention-OMH, Advocacy Services-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'Home Based Crisis Intervention-OMH, Family Support Services-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'Home Based Crisis Intervention-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'Home Based Family Treatment-OMH',
                'Mental Health Services, Vocational',
                'MICA Network-OMH',
                'Mobile Adolescent Therapy',
                'Non-Medicaid Care Coordination (OMH)-OMH',
                'Non-Medicaid Care Coordination; (Non-Licensed Program)-OMH',
                'Non-Medicaid Care Coordination; (Non-Licensed Program)-OMH, Assertive Community Treatment-OMH',
                'Non-Medicaid Care Coordination; (Non-Licensed Program)-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'On-Site Rehabilitation-OMH',
                'On-Site Rehabilitation-OMH, Assisted Competitive Employment-OMH, Advocacy Services-OMH, Supported SRO-OMH',
                'On-Site Rehabilitation-OMH, Supported Housing-OMH',
                'On-Site Rehabilitation-OMH, Supported SRO-OMH',
                'On-Site Rehabilitation-OMH, Vocational Services-OMH',
                'Outreach-OMH',
                'Outreach-OMH, Advocacy Services-OMH, Crisis/Respite Beds-OMH',
                'Outreach-OMH, Psychosocial Club-OMH, Supported SRO-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'Psychosocial Club-OMH',
                'Psychosocial Club-OMH, Clinic Treatment-OMH',
                'Psychosocial Club-OMH, Crisis/Respite Beds-OMH',
                'Psychosocial Club-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'Psychosocial Club-OMH, Supported SRO-OMH',
                'Recovery Center-OMH',
                'School-OMH',
                'Self-Help-OMH',
                'Self-Help-OMH, Advocacy Services-OMH',
                'Self-Help-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'Self-Help-OMH, Non-Medicaid Care Coordination; (Non-Licensed Program)-OMH, Advocacy Services-OMH'
            )
            THEN 'Mental Health'
        WHEN
            program_name IN
            (
                'Adult Outreach Service',
                'Drop-in Center',
                'Drop-In Centers',
                'Homebase Homelessness Prevention',
                'Housing Support',
                'NY NY III Supported Housing-OASAS',
                'NY NY III Supported Housing-OASAS, Supported Housing-OASAS',
                'NY NY III Supported Housing-OASAS, Supported Housing-OMH',
                'NY NY III Supported Housing-OMH',
                'NY NY III Supported Housing-OMH, Supported Housing-OMH',
                'Rapid Re-Housing',
                'Shelter Intake',
                'Shelter/Shelter Services',
                'Supported Housing-OMH',
                'Supported Housing-OMH, Assertive Community Treatment-OMH',
                'Supported Housing-OMH, Assertive Community Treatment-OMH, Supported SRO-OMH, Respite-OMH',
                'Supported Housing-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'Supported Housing-OMH, Supported SRO-OMH',
                'Supported Housing-OMH, Supported SRO-OMH, Non-Medicaid Care Coordination (OMH)-OMH',
                'Supported SRO-OMH',
                'Supported SRO-OMH, Crisis/Respite Beds-OMH',
                'Supported SRO-OMH, On-Site Rehabilitation-OMH, Assisted Competitive Employment-OMH, Homeless Placement Services (Non-Licensed Program)-OMH'
            )
            THEN 'Non-residential Housing and Homeless Services'
        WHEN
            program_name IN
            (
                'Home Care/Attendant/Maker and Housekeeping Services',
                'Compulsive Gambling Services',
                'Special Demo',
                'Special Demo - City Council-OASAS',
                'Special Demo - City Council-OASAS-OMH',
                'Special Demo - City Council-OMH',
                'Special Demo - City Council-OPWDD',
                'Special Demo - City Council-OPWDD-OASAS-OMH',
                'Special Demo - City Council-OPWDD-OMH'
            )
            THEN 'Other Health Care'
        WHEN
            program_name IN
            (
                'Caregiver Services',
                'Case Management',
                'Elder Justice',
                'Home Care Services',
                'Home-Delivered Meals',
                'Legal Assistance',
                'Naturally Occurring Retirement Community (NORC)',
                'NY Connects',
                'Seniors',
                'Transportation'
            )
            THEN 'Senior Services'
        WHEN
            program_name IN
            (
                'Food Pantry/Meal Services',
                'Food Pantry'
            )
            THEN 'Soup Kitchens and Food Pantries'
        WHEN
            program_name IN
            (
                'CareerAdvance',
                'CareerCompass',
                'ECHOES',
                'Employment Focused Services',
                'Job Services',
                'Jobs Plus',
                'Justice Plus',
                'Job Placement Initiative-OASAS',
                'Neighborhood Employment Services',
                'NeON',
                'NeON Arts',
                'NYC Business Solutions',
                'Placement Services',
                'Vocational Services-OMH',
                'WeCARE',
                'Workforce 1 Career Centers',
                'Works Plus',
                'YouthPathways'
            )
            THEN 'Workforce Development'
        WHEN
            program_name IN
            (
                'Adolescent Literacy',
                'Advance & Earn',
                'City Council Awards',
                'Intern & Earn',
                'Ladders for Leaders (LFL)',
                'Learn & Earn',
                'Opportunity Youth: Supported Work Experience',
                'Summer Youth Employment Program (SYEP)',
                'Summer Youth Employment Program (SYEP) (ages 14-15)',
                'Summer Youth Employment Program (SYEP) (ages 16-24)',
                'SYEP and Work, Learn & Grow (WLG)',
                'SYEP Career FIRST',
                'SYEP CareerReady',
                'SYEP Emerging Leaders: Leaders Influencing Tomorrow',
                'SYEP Map to $uccess',
                'Train & Earn',
                'Work, Learn & Grow (WLG)'
            )
            THEN 'Youth Centers, Literacy Programs, and Job Training Services'
        WHEN
            program_name IN
            (
                'Adult Protective Services',
                'Clinic Treatment Facility-OPWDD',
                'Epilepsy Services-OPWDD',
                'Homemaker Services-OPWDD',
                'Recreation-OPWDD',
                'Recreation-OPWDD, Case Management-OPWDD, Summer Camp-OPWDD',
                'Transitional Employment-OPWDD'
            )
            THEN 'Programs for People with Disabilities'
    END) AS facsubgrp,
    provider_name AS opname,
    NULL AS opabbrev,
    'NYC' || agency_name AS overabbrev,
    NULL AS capacity,
    NULL AS captype,
    NULL AS wkb_geometry,
    geo_1b,
    geo_bl,
    geo_bn
INTO _moeo_socialservicesitelocations
FROM moeo_socialservicesitelocations
WHERE
    uid IN (SELECT uid FROM tmp)
    AND program_name !~* 'Home Delivered Meals|senior center|CONDOM DISTRIBUTION SERVICES|GROWING UP NYC INITIATIVE SUPPORT SERVICES|PLANNING AND EVALUATION [BASE]|TO BE DETERMINED - UNKNOWN';

CALL append_to_facdb_base('_moeo_socialservicesitelocations');
