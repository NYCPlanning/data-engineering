DROP TABLE IF EXISTS _dcp_colp;
WITH _dcp_colp_tmp AS (
    SELECT
        uid,
        source,
        (
            CASE
                WHEN (
                    parcelname = ' '
                    OR parcelname IS NULL
                )
                AND usetype ~* 'office' THEN 'Offices'
                WHEN (
                    parcelname = ' '
                    OR parcelname IS NULL
                )
                AND usetype ~* 'no use'
                AND ownership = 'C' THEN 'City Owned Property'
                WHEN
                    parcelname <> ' '
                    AND parcelname IS NOT NULL THEN initcap(parcelname)
                ELSE initcap(replace(usetype, 'OTHER ', ''))
            END
        ) AS facname,
        hnum AS addressnum,
        sname AS streetname,
        address,
        NULL AS city,
        NULL AS zipcode,
        NULL AS boro,
        borough AS borocode,
        nullif(geo_1b -> 'result' ->> 'geo_bin', '') AS bin,
        left(bbl, 10) AS bbl,
        (
            CASE
                WHEN
                    parcelname ~* 'PRECINCT'
                    AND usecode = '0500' THEN 'Police Station'
                ELSE initcap(replace(usetype, 'OTHER ', ''))
            END
        ) AS factype,
        (
            CASE
                -- Admin of Gov
                WHEN
                    usetype LIKE '%AGREEMENT%'
                    OR usetype LIKE '%DISPOSITION%'
                    OR usetype LIKE '%COMMITMENT%'
                    OR excatdesc LIKE '%PRIVATE%' THEN 'Properties Leased or Licensed to Non-public Entities'
                WHEN usetype LIKE '%SECURITY%' THEN 'Miscellaneous Use'
                WHEN
                    usetype LIKE '%PARKING%'
                    AND usetype NOT LIKE '%MUNICIPAL%' THEN 'City Agency Parking'
                WHEN usetype LIKE '%STORAGE%' THEN 'Storage'
                WHEN usetype LIKE '%CUSTODIAL%' THEN 'Custodial'
                WHEN usetype LIKE '%GARAGE%' THEN 'Maintenance and Garages'
                WHEN usetype LIKE '%OFFICE%' THEN 'City Government Offices'
                WHEN usetype LIKE '%MAINTENANCE%' THEN 'Maintenance and Garages'
                WHEN usetype LIKE '%NO USE%' THEN 'Miscellaneous Use'
                WHEN usetype LIKE '%MISCELLANEOUS USE%' THEN 'Miscellaneous Use'
                WHEN
                    usetype LIKE '%OTHER HEALTH%'
                    AND parcelname LIKE '%ANIMAL%' THEN 'Miscellaneous Use'
                WHEN
                    agency LIKE '%DCA%'
                    AND usetype LIKE '%OTHER%' THEN 'Miscellaneous Use'
                WHEN usetype LIKE '%UNDEVELOPED%' THEN 'Miscellaneous Use'
                WHEN (
                    usetype LIKE '%TRAINING%'
                    OR usetype LIKE '%TESTING%'
                )
                AND usetype NOT LIKE '%LABORATORY%' THEN 'Training and Testing' -- Trans and Infra
                WHEN usetype LIKE '%MUNICIPAL PARKING%' THEN 'Parking Lots and Garages'
                WHEN usetype LIKE '%MARKET%' THEN 'Wholesale Markets'
                WHEN usetype LIKE '%MATERIAL PROCESSING%' THEN 'Material Supplies'
                WHEN usetype LIKE '%ASPHALT%' THEN 'Material Supplies'
                WHEN usetype LIKE '%AIRPORT%' THEN 'Airports and Heliports'
                WHEN
                    usetype LIKE '%ROAD/HIGHWAY%'
                    OR usetype LIKE '%TRANSIT WAY%'
                    OR usetype LIKE '%OTHER TRANSPORTATION%' THEN 'Other Transportation'
                WHEN
                    agency LIKE '%DEP%'
                    AND (
                        usetype LIKE '%WATER SUPPLY%'
                        OR usetype LIKE '%RESERVOIR%'
                        OR usetype LIKE '%AQUEDUCT%'
                    ) THEN 'Water Supply'
                WHEN
                    agency LIKE '%DEP%'
                    AND usetype NOT LIKE '%NATURE AREA%'
                    AND usetype NOT LIKE '%NATURAL AREA%'
                    AND usetype NOT LIKE '%OPEN SPACE%' THEN 'Wastewater and Pollution Control'
                WHEN usetype LIKE '%WASTEWATER%' THEN 'Wastewater and Pollution Control'
                WHEN
                    usetype LIKE '%LANDFILL%'
                    OR usetype LIKE '%SOLID WASTE INCINERATOR%' THEN 'Solid Waste Processing'
                WHEN
                    usetype LIKE '%SOLID WASTE TRANSFER%'
                    OR (
                        agency LIKE '%SANIT%'
                        AND usetype LIKE '%SANITATION SECTION%'
                    ) THEN 'Solid Waste Transfer and Carting'
                WHEN
                    usetype LIKE '%ANTENNA%'
                    OR usetype LIKE '%TELE/COMP%' THEN 'Telecommunications'
                WHEN
                    usetype LIKE '%PIER - MARITIME%'
                    OR usetype LIKE '%FERRY%'
                    OR usetype LIKE '%WATERFRONT TRANSPORTATION%'
                    OR usetype LIKE '%MARINA%' THEN 'Ports and Ferry Landings'
                WHEN
                    usetype LIKE '%RAIL%'
                    OR (
                        usetype LIKE '%TRANSIT%'
                        AND usetype NOT LIKE '%TRANSITIONAL%'
                    ) THEN 'Rail Yards and Maintenance'
                WHEN usetype LIKE '%BUS%' THEN 'Bus Depots and Terminals' -- Health and Human
                WHEN agency LIKE '%HHC%' THEN 'Hospitals and Clinics'
                WHEN usetype LIKE '%HOSPITAL%' THEN 'Hospitals and Clinics'
                WHEN usetype LIKE '%AMBULATORY HEALTH%' THEN 'Hospitals and Clinics'
                WHEN agency LIKE '%OCME%' THEN 'Other Health Care'
                WHEN
                    agency LIKE '%ACS%'
                    AND usetype LIKE '%HOUSING%' THEN 'Shelters and Transitional Housing'
                WHEN agency LIKE '%AGING%' THEN 'Senior Services'
                WHEN (
                    agency LIKE '%DHS%'
                    OR agency LIKE '%HRA%'
                )
                AND (
                    usetype LIKE '%RESIDENTIAL%'
                    OR usetype LIKE '%TRANSITIONAL HOUSING%'
                ) THEN 'Shelters and Transitional Housing'
                WHEN
                    agency LIKE '%DHS%'
                    AND usetype NOT LIKE '%OPEN SPACE%' THEN 'Non-residential Housing and Homeless Services'
                WHEN (
                    agency LIKE '%NYCHA%'
                    OR agency LIKE '%HPD%'
                )
                AND usetype LIKE '%RESIDENTIAL%' THEN 'Public or Affordable Housing'
                WHEN
                    usetype LIKE '%COMMUNITY CENTER%'
                    OR (
                        agency LIKE '%HRA%'
                        AND parcelname LIKE '%CENTER%'
                    ) THEN 'Community Centers and Community Programs' -- Parks, Cultural
                WHEN usetype LIKE '%LIBRARY%' THEN 'Public Libraries'
                WHEN usetype LIKE '%MUSEUM%' THEN 'Museums'
                WHEN usetype LIKE '%CULTURAL%' THEN 'Other Cultural Institutions'
                WHEN usetype LIKE '%ZOO%' THEN 'Other Cultural Institutions'
                WHEN usetype LIKE '%CEMETERY%' THEN 'Cemeteries'
                WHEN
                    agency LIKE '%CULT%'
                    AND usetype LIKE '%MUSEUM%' THEN 'Museums'
                WHEN agency LIKE '%CULT%' THEN 'Other Cultural Institutions'
                WHEN
                    usetype LIKE '%NATURAL AREA%'
                    OR (
                        usetype LIKE '%OPEN SPACE%'
                        AND agency LIKE '%DEP%'
                    ) THEN 'Preserves and Conservation Areas'
                WHEN usetype LIKE '%BOTANICAL GARDENS%' THEN 'Other Cultural Institutions'
                WHEN usetype LIKE '%GARDEN%' THEN 'Gardens'
                WHEN
                    agency LIKE '%PARKS%'
                    AND usetype LIKE '%OPEN SPACE%' THEN 'Streetscapes, Plazas, and Malls'
                WHEN usetype = 'MALL/TRIANGLE/HIGHWAY STRIP/PARK STRIP' THEN 'Streetscapes, Plazas, and Malls'
                WHEN usetype LIKE '%PARK%' THEN 'Parks'
                WHEN
                    usetype LIKE '%PLAZA%'
                    OR usetype LIKE '%SITTING AREA%' THEN 'Streetscapes, Plazas, and Malls'
                WHEN
                    usetype LIKE '%PLAYGROUND%'
                    OR usetype LIKE '%SPORTS%'
                    OR usetype LIKE '%TENNIS COURT%'
                    OR usetype LIKE '%PLAY AREA%'
                    OR usetype LIKE '%RECREATION%'
                    OR usetype LIKE '%BEACH%'
                    OR usetype LIKE '%PLAYING FIELD%'
                    OR usetype LIKE '%GOLF COURSE%'
                    OR usetype LIKE '%POOL%'
                    OR usetype LIKE '%STADIUM%' THEN 'Recreation and Waterfront Sites'
                WHEN
                    usetype LIKE '%THEATER%'
                    AND agency LIKE '%DSBS%' THEN 'Other Cultural Institutions' -- Public Safety, Justice etc
                WHEN
                    agency LIKE '%ACS%'
                    AND usetype LIKE '%DETENTION%' THEN 'Detention and Correctional'
                WHEN
                    agency LIKE '%CORR%'
                    AND usetype LIKE '%COURT%' THEN 'Courthouses and Judicial'
                WHEN
                    agency LIKE '%COURT%'
                    AND usetype LIKE '%COURT%' THEN 'Courthouses and Judicial'
                WHEN
                    agency LIKE '%OCA%'
                    AND usetype LIKE '%COURT%' THEN 'Courthouses and Judicial'
                WHEN agency LIKE '%CORR%' THEN 'Detention and Correctional'
                WHEN usetype LIKE '%AMBULANCE%' THEN 'Other Emergency Services'
                WHEN usetype LIKE '%EMERGENCY MEDICAL%' THEN 'Other Emergency Services'
                WHEN usetype LIKE '%FIREHOUSE%' THEN 'Fire Services'
                WHEN usetype LIKE '%POLICE STATION%' THEN 'Police Services'
                WHEN usetype LIKE '%PUBLIC SAFETY%' THEN 'Other Public Safety'
                WHEN agency LIKE '%OCME%' THEN 'Forensics' -- Education, Children, Youth
                WHEN usetype LIKE '%UNIVERSITY%' THEN 'Colleges or Universities'
                WHEN usetype LIKE '%EARLY CHILDHOOD%' THEN 'Day Care'
                WHEN usetype LIKE '%DAY CARE%' THEN 'Day Care'
                WHEN
                    agency LIKE '%ACS%'
                    AND usetype LIKE '%RESIDENTIAL%' THEN 'Foster Care Services and Residential Care'
                WHEN agency LIKE '%ACS%' THEN 'Day Care'
                WHEN
                    agency LIKE '%EDUC%'
                    AND usetype LIKE '%PLAY AREA%' THEN 'Public K-12 Schools'
                WHEN usetype LIKE '%HIGH SCHOOL%' THEN 'Public K-12 Schools'
                WHEN
                    agency LIKE '%CUNY%'
                    AND usetype NOT LIKE '%OPEN SPACE%' THEN 'Colleges or Universities'
                WHEN
                    agency LIKE '%EDUC%'
                    AND usetype LIKE '%SCHOOL%' THEN 'Public K-12 Schools'
                WHEN usetype LIKE '%EDUCATIONAL SKILLS%' THEN 'Public K-12 Schools'
                ELSE 'Miscellaneous Use'
            END
        ) AS facsubgrp,
        (
            CASE
                WHEN agency = 'ACS' THEN 'NYC Administration for Childrens Services'
                WHEN agency = 'ACTRY' THEN 'NYC Office of the Actuary'
                WHEN agency = 'AGING' THEN 'NYC Department for the Aging'
                WHEN agency = 'BIC' THEN 'NYC Business Integrity Commission'
                WHEN agency = 'BLDGS' THEN 'NYC Department of Buildings'
                WHEN agency = 'BP-BK' THEN 'NYC Borough President - Brooklyn'
                WHEN agency = 'BP-BX' THEN 'NYC Borough President - Bronx'
                WHEN agency = 'BP-MN' THEN 'NYC Borough President - Manhattan'
                WHEN agency = 'BP-QN' THEN 'NYC Borough President - Queens'
                WHEN agency = 'BP-SI' THEN 'NYC Borough President - Staten Island'
                WHEN agency = 'BPL' THEN 'Brooklyn Public Library'
                WHEN agency = 'BSA' THEN 'NYC Board of Standards and Appeals'
                WHEN agency LIKE 'CB%'
                    THEN replace(
                        concat(
                            'NYC ',
                            initcap(borough),
                            ' Community Board ',
                            right(agency, 2)
                        ),
                        ' 0',
                        ' '
                    )
                WHEN agency = 'CCRB' THEN 'NYC Civilian Complaint Review Board'
                WHEN agency = 'CEO' THEN 'NYC Center for Economic Opportunity'
                WHEN agency = 'CFB' THEN 'NYC Campaign Finance Board'
                WHEN agency = 'CIVIL' THEN 'NYC Civil Service Commission'
                WHEN agency = 'CLERK' THEN 'NYC Office of the City Clerk'
                WHEN agency = 'COIB' THEN 'NYC Conflict of Interest Board'
                WHEN agency = 'COMPT' THEN 'NYC Office of the Comptroller'
                WHEN agency = 'CORR' THEN 'NYC Department of Correction'
                WHEN agency = 'COUNC' THEN 'NYC City Council'
                WHEN agency = 'COURT' THEN 'NYS Unified Court System'
                WHEN agency = 'CULT' THEN 'NYC Department of Cultural Affairs'
                WHEN agency = 'CUNY' THEN 'City University of New York'
                WHEN agency = 'DA-BK' THEN 'NYC District Attorney - Brooklyn'
                WHEN agency = 'DA-BX' THEN 'NYC District Attorney - Bronx '
                WHEN agency = 'DA-MN' THEN 'NYC District Attorney - Manhattan'
                WHEN agency = 'DA-QN' THEN 'NYC District Attorney - Queens'
                WHEN agency = 'DA-SI' THEN 'NYC District Attorney - Staten Island'
                WHEN agency = 'DA-SP' THEN 'NYC District Attorney - Office Special Narcotics'
                WHEN agency = 'DCA' THEN 'NYC Department of Consumer Affairs'
                WHEN agency = 'DCAS' THEN 'NYC Department of Citywide Administrative Services'
                WHEN agency = 'DDC' THEN 'NYC Department of Design and Construction'
                WHEN agency = 'DEP' THEN 'NYC Department of Environmental Protection'
                WHEN agency = 'DHS' THEN 'NYC Department of Homeless Services'
                WHEN agency = 'DOI' THEN 'NYC Department of Investigation'
                WHEN agency = 'DOITT' THEN 'NYC Department of Information Technology and Telecommunications'
                WHEN agency = 'DORIS' THEN 'NYC Department of Records and Information Services'
                WHEN agency = 'DOT' THEN 'NYC Department of Transportation'
                WHEN agency = 'DSBS' THEN 'NYC Department of Small Business Services'
                WHEN agency = 'DYCD' THEN 'NYC Department of Youth and Community Development'
                WHEN agency = 'EDC' THEN 'NYC Economic Development Corporation'
                WHEN agency = 'EDUC' THEN 'NYC Department of Education'
                WHEN agency = 'ELECT' THEN 'NYC Board of Elections'
                WHEN agency = 'FINAN' THEN 'NYC Department of Finance'
                WHEN agency = 'FIRE' THEN 'NYC Fire Department'
                WHEN agency = 'FISA' THEN 'NYC Financial Information Services Agency'
                WHEN agency = 'HHC' THEN 'NYC Health and Hospitals Corporation'
                WHEN agency = 'HLTH' THEN 'NYC Department of Health and Mental Hygiene'
                WHEN agency = 'HPD' THEN 'NYC Department of Housing Preservation and Development'
                WHEN agency = 'HRA' THEN 'NYC Human Resources Administration/Department of Social Services'
                WHEN agency = 'HUMRT' THEN 'NYC Commission on Human Rights'
                WHEN agency = 'HYDC' THEN 'Hudson Yards Development Corporation'
                WHEN agency = 'IBO' THEN 'NYC Independent Budget Office'
                WHEN agency = 'LAW' THEN 'NYC Law Department'
                WHEN agency = 'LDMKS' THEN 'NYC Landmarks Preservation Commission'
                WHEN agency = 'MAYOR' THEN 'NYC Office of the Mayor'
                WHEN agency = 'MOME' THEN 'NYC Office of the Mayor'
                WHEN agency = 'MTA' THEN 'Metropolitan Transportation Authority'
                WHEN agency = 'NYCHA' THEN 'NYC Housing Authority'
                WHEN agency = 'NYCTA' THEN 'Metropolitan Transportation Authority'
                WHEN agency = 'NYPD' THEN 'NYC Police Department'
                WHEN agency = 'NYPL' THEN 'New York Public Library'
                WHEN agency = 'OATH' THEN 'NYC Office of Administrative Trials and Hearings'
                WHEN agency = 'OCA' THEN 'NYC Office of Court Administration'
                WHEN agency = 'OCB' THEN 'NYC Office of Collective Bargaining'
                WHEN agency = 'OCME' THEN 'NYC Office of the Medical Examiner'
                WHEN agency = 'OEM' THEN 'NYC Office of Emergency Management'
                WHEN agency = 'OLR' THEN 'NYC Office of Labor Relations'
                WHEN agency = 'OMB' THEN 'NYC Office of Management and Budget'
                WHEN agency = 'OPA' THEN 'NYC Office of Payroll Administration'
                WHEN agency = 'PA-BK' THEN 'NYC Public Administrators Office - Brooklyn'
                WHEN agency = 'PA-BX' THEN 'NYC Public Administrators Office - Bronx'
                WHEN agency = 'PA-MN' THEN 'NYC Public Administrators Office - Manhattan'
                WHEN agency = 'PA-QN' THEN 'NYC Public Administrators Office - Queens'
                WHEN agency = 'PA-SI' THEN 'NYC Public Administrators Office - Staten Island'
                WHEN agency = 'PARKS' THEN 'NYC Department of Parks and Recreation'
                WHEN agency = 'PBADV' THEN 'NYC Office of Public Advocate'
                WHEN agency = 'PLAN' THEN 'NYC Department of City Planning'
                WHEN agency = 'PRIV' THEN 'Non-public'
                WHEN agency = 'PROB' THEN 'NYC Department of Probation'
                WHEN agency = 'QPL' THEN 'Queens Public Library'
                WHEN agency = 'SANIT' THEN 'NYC Department of Sanitation'
                WHEN agency = 'TAXCM' THEN 'NYC Tax Commission'
                WHEN agency = 'TBTA' THEN 'Metropolitan Transportation Authority'
                WHEN agency = 'TLC' THEN 'NYC Taxi and Limousine Commission'
                WHEN agency = 'UNKN' THEN 'NYC Unknown'
            END
        ) AS opname,
        (
            CASE
                WHEN agency = 'HYDC' THEN 'HYDC'
                WHEN agency = 'MTA' THEN 'MTA'
                WHEN agency = 'NYCTA' THEN 'MTA'
                WHEN agency = 'TBTA' THEN 'MTA'
                WHEN agency = 'PARKS' THEN 'NYCDPR'
                WHEN agency = 'BLDGS' THEN 'NYCDOB'
                WHEN agency = 'BPL' THEN 'BPL'
                WHEN agency = 'NYPL' THEN 'NYPL'
                WHEN agency = 'QPL' THEN 'QPL'
                WHEN agency = 'SANIT' THEN 'NYCDSNY'
                WHEN agency = 'AGING' THEN 'NYCDFTA'
                WHEN agency = 'EDUC' THEN 'NYCDOE'
                WHEN agency = 'CULT' THEN 'NYCDCLA'
                WHEN agency = 'CORR' THEN 'NYCDOC'
                WHEN agency = 'HLTH' THEN 'NYCDOHMH'
                WHEN agency = 'ELECT' THEN 'BOENY'
                WHEN agency = 'CIVIL' THEN 'NYCCCSC'
                WHEN agency = 'HUMRT' THEN 'NYCCCHR'
                WHEN agency = 'COUNC' THEN 'NYCC'
                WHEN agency = 'PLAN' THEN 'NYCDCP'
                WHEN agency = 'FINAN' THEN 'NYCDOF'
                WHEN agency = 'PROB' THEN 'NYCDOP'
                WHEN agency = 'DSBS' THEN 'NYCSBS'
                WHEN agency = 'FIRE' THEN 'FDNY'
                WHEN agency = 'NYPD' THEN 'NYPD'
                WHEN agency = 'HRA' THEN 'NYCHRA'
                WHEN agency = 'DA-SP' THEN 'NYCDA-SNP'
                WHEN agency = 'LDMKS' THEN 'NYCLPC'
                WHEN agency = 'OEM' THEN 'NYCEM'
                WHEN agency = 'PBADV' THEN 'NYCPA'
                WHEN agency = 'ACTRY' THEN 'NYCACT'
                WHEN agency = 'COMPT' THEN 'NYCCOMP'
                WHEN agency = 'MAYOR' THEN 'NYCOOM'
                WHEN agency = 'MOME' THEN 'NYCOOM'
                WHEN agency = 'TAX' THEN 'NYCTC'
                WHEN agency = 'COURT' THEN 'NYCOURTS'
                WHEN agency = 'CUNY' THEN 'CUNY'
                WHEN agency = 'CNTYC' THEN 'CNTYC'
                WHEN agency = 'PRIV' THEN 'Non-public'
                WHEN agency = 'UNKN' THEN 'NYC-Unknown'
                ELSE concat('NYC', agency)
            END
        ) AS opabbrev,
        (CASE
            WHEN agency = 'HYDC' THEN 'HYDC'
            WHEN agency = 'MTA' THEN 'MTA'
            WHEN agency = 'NYCTA' THEN 'MTA'
            WHEN agency = 'TBTA' THEN 'MTA'
            WHEN agency = 'PARKS' THEN 'NYCDPR'
            WHEN agency = 'BLDGS' THEN 'NYCDOB'
            WHEN agency = 'BPL' THEN 'BPL'
            WHEN agency = 'NYPL' THEN 'NYPL'
            WHEN agency = 'QPL' THEN 'QPL'
            WHEN agency = 'SANIT' THEN 'NYCDSNY'
            WHEN agency = 'AGING' THEN 'NYCDFTA'
            WHEN agency = 'EDUC' THEN 'NYCDOE'
            WHEN agency = 'CULT' THEN 'NYCDCLA'
            WHEN agency = 'CORR' THEN 'NYCDOC'
            WHEN agency = 'HLTH' THEN 'NYCDOHMH'
            WHEN agency = 'ELECT' THEN 'BOENY'
            WHEN agency = 'CIVIL' THEN 'NYCCCSC'
            WHEN agency = 'HUMRT' THEN 'NYCCCHR'
            WHEN agency = 'COUNC' THEN 'NYCC'
            WHEN agency = 'PLAN' THEN 'NYCDCP'
            WHEN agency = 'FINAN' THEN 'NYCDOF'
            WHEN agency = 'PROB' THEN 'NYCDOP'
            WHEN agency = 'DSBS' THEN 'NYCSBS'
            WHEN agency = 'FIRE' THEN 'FDNY'
            WHEN agency = 'NYPD' THEN 'NYPD'
            WHEN agency = 'HRA' THEN 'NYCHRA'
            WHEN agency = 'DA-SP' THEN 'NYCDA-SNP'
            WHEN agency = 'LDMKS' THEN 'NYCLPC'
            WHEN agency = 'OEM' THEN 'NYCEM'
            WHEN agency = 'PBADV' THEN 'NYCPA'
            WHEN agency = 'ACTRY' THEN 'NYCACT'
            WHEN agency = 'COMPT' THEN 'NYCCOMP'
            WHEN agency = 'MAYOR' THEN 'NYCOOM'
            WHEN agency = 'MOME' THEN 'NYCOOM'
            WHEN agency = 'TAX' THEN 'NYCTC'
            WHEN agency = 'COURT' THEN 'NYCOURTS'
            WHEN agency = 'CUNY' THEN 'CUNY'
            WHEN agency = 'CNTYC' THEN 'CNTYC'
            WHEN agency = 'PRIV' THEN 'Non-public'
            WHEN agency = 'UNKN' THEN 'NYC-Unknown'
            ELSE concat('NYC', agency)
        END) AS overabbrev,
        NULL AS capacity,
        NULL AS captype,
        st_transform(geom::geometry, 4326) AS wkb_geometry,
        NULL AS geo_1b,
        NULL AS geo_bl,
        NULL AS geo_bn
    FROM dcp_colp
    WHERE
        (
            (
                concat(category, expandcat) IN ('11', '12', '14', '16', '17', '28', '29', '38')
                AND usecode NOT IN (
                    '0520',
                    '0800',
                    '0813',
                    '0814',
                    '0870',
                    '0900',
                    '0939',
                    '1139',
                    '1200',
                    '1229',
                    '1300',
                    '1342',
                    '1350',
                    '1400',
                    '1420',
                    '1500',
                    '1510',
                    '1520',
                    '1530',
                    '1900'
                )
                AND usecode NOT LIKE '02%'
            )
            OR usecode = '0230'
            OR usecode = '1320'
            OR usecode = '1321'
        )
        AND (
            (
                agency <> 'NYCHA'
                AND agency <> 'HPD'
                AND usetype <> 'ROAD/HIGHWAY'
                AND usetype <> 'TRANSIT WAY'
                AND usetype NOT LIKE '%WATER SUPPLY%'
                AND usetype NOT LIKE '%RESERVOIR%'
                AND usetype NOT LIKE '%AQUEDUCT%'
                AND agency <> 'DHS'
            )
            OR (agency = 'DHS' AND usetype NOT LIKE '%RESIDENTIAL%' AND usetype NOT LIKE '%HOUSING%')
            OR (agency = 'HRA' AND usetype NOT LIKE '%RESIDENTIAL%' AND usetype NOT LIKE '%HOUSING%')
            OR (agency = 'ACS' AND usetype NOT LIKE '%RESIDENTIAL%' AND usetype NOT LIKE '%HOUSING%')
        )
),
duplicate_offices AS (
    SELECT uid
    FROM _dcp_colp_tmp
    WHERE
        bbl IN
        (
            SELECT filtered.bbl
            FROM _dcp_colp_tmp AS filtered
            WHERE filtered.factype = 'Office' OR filtered.factype = 'Agency Office'
            GROUP BY filtered.bbl, filtered.opabbrev
            HAVING count(DISTINCT filtered.factype) > 1
        )
        AND factype = 'Office'
)
SELECT DISTINCT ON (facname, factype, facsubgrp, opabbrev, left(bbl, 6)) *
INTO _dcp_colp
FROM _dcp_colp_tmp
WHERE uid NOT IN (SELECT uid FROM duplicate_offices);

CALL append_to_facdb_base('_dcp_colp');
