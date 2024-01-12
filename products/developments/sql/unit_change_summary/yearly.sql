/*
DESCRIPTION:
    Creates an unaggregated table as input for the spatial aggregates.
    Includes completions split by year.

INPUTS:
    FINAL_devdb
OUTPUTS:
    YEARLY_devdb
*/

DROP TABLE IF EXISTS YEARLY_DEVDB;
CREATE TABLE YEARLY_DEVDB AS
SELECT
    JOB_NUMBER,
    (CASE
        WHEN BORO = '1' THEN 'Manhattan'
        WHEN BORO = '2' THEN 'Bronx'
        WHEN BORO = '3' THEN 'Brooklyn'
        WHEN BORO = '4' THEN 'Queens'
        WHEN BORO = '5' THEN 'Staten Island'
    END) AS BORO,
    BORO AS BOROCODE,
    bctcb{{ decade }}::TEXT,
    cenblock{{ decade }}::TEXT,
    bct{{ decade }}::TEXT,
    LEFT(cenblock{{ decade }}, 11) AS centract{{ decade }},
    nta{{ decade }}::TEXT,
    ntaname{{ decade }},

    cdta{{ decade }}::TEXT,
    cdtaname{{ decade }},

    COMUNITYDIST::TEXT,
    COUNCILDIST::TEXT,

    CASE
        WHEN
            COMPLETE_YEAR = '2020' AND DATE_COMPLETE > '2020-03-31'::DATE
            AND JOB_INACTIVE IS NULL
            THEN CLASSA_NET
    END AS COMP2020AP,
    
    {%- for year in years %}
        CASE
            WHEN COMPLETE_YEAR = '{{ year }}' AND JOB_INACTIVE IS NULL
                THEN CLASSA_NET
        END AS comp{{ year }},
    {% endfor %}

    CASE
        WHEN
            DATE_COMPLETE > '2020-03-31'::DATE AND DATE_COMPLETE < '{{ CAPTURE_DATE }}'::DATE
            AND JOB_INACTIVE IS NULL
            THEN CLASSA_NET
    END AS SINCE_CEN20,

    CASE
        WHEN
            JOB_STATUS = '1. Filed Application'
            AND JOB_INACTIVE IS NULL
            THEN CLASSA_NET
    END AS FILED,

    CASE
        WHEN
            JOB_STATUS = '2. Approved Application'
            AND JOB_INACTIVE IS NULL
            THEN CLASSA_NET
    END AS APPROVED,

    CASE
        WHEN
            JOB_STATUS = '3. Permitted for Construction'
            AND JOB_INACTIVE IS NULL
            THEN CLASSA_NET
    END AS PERMITTED,

    CASE
        WHEN
            JOB_STATUS = '9. Withdrawn'
            AND DATE_LASTUPDT::DATE > '2009-12-31'::DATE
            THEN CLASSA_NET
    END AS WITHDRAWN,

    CASE
        WHEN
            JOB_STATUS != '9. Withdrawn'
            AND JOB_INACTIVE ~* 'Inactive'
            AND DATE_LASTUPDT::DATE > '2009-12-31'::DATE
            THEN CLASSA_NET
    END AS INACTIVE

FROM FINAL_DEVDB;
