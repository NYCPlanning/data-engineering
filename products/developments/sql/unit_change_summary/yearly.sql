/*
DESCRIPTION:
    Creates an unaggregated table as input for the spatial aggregates.
    Includes completions split by year.

INPUTS:
    FINAL_devdb
OUTPUTS:
    YEARLY_devdb
*/

DROP TABLE IF EXISTS yearly_devdb;
CREATE TABLE yearly_devdb AS
SELECT
    job_number,
    (CASE
        WHEN boro = '1' THEN 'Manhattan'
        WHEN boro = '2' THEN 'Bronx'
        WHEN boro = '3' THEN 'Brooklyn'
        WHEN boro = '4' THEN 'Queens'
        WHEN boro = '5' THEN 'Staten Island'
    END) AS boro,
    boro AS borocode,
    bctcb{{ decade }}::text,
    cenblock{{ decade }}::text,
    bct{{ decade }}::text,
    left(cenblock{{ decade }}, 11) AS centract{{ decade }},
    nta{{ decade }}::text,
    ntaname{{ decade }},

    cdta{{ decade }}::text,
    cdtaname{{ decade }},

    comunitydist::text,
    councildist::text,

    CASE
        WHEN
            complete_year = '2020' AND date_complete > '2020-04-01'::date
            AND job_inactive IS NULL
            THEN classa_net
    END AS comp2020ap,
    
    {%- for year in years %}
        CASE
            WHEN complete_year = '{{ year }}' AND job_inactive IS NULL
                THEN classa_net
        END AS comp{{ year }},
    {% endfor %}

    CASE
        WHEN
            date_complete > '2020-04-01'::date AND date_complete < '{{ CAPTURE_DATE }}'::date
            AND job_inactive IS NULL
            THEN classa_net
    END AS since_cen20,

    CASE
        WHEN
            job_status = '1. Filed Application'
            AND job_inactive IS NULL
            THEN classa_net
    END AS filed,

    CASE
        WHEN
            job_status = '2. Approved Application'
            AND job_inactive IS NULL
            THEN classa_net
    END AS approved,

    CASE
        WHEN
            job_status = '3. Permitted for Construction'
            AND job_inactive IS NULL
            THEN classa_net
    END AS permitted,

    CASE
        WHEN
            job_status = '9. Withdrawn'
            AND date_lastupdt::date > '2009-12-31'::date
            THEN classa_net
    END AS withdrawn,

    CASE
        WHEN
            job_status != '9. Withdrawn'
            AND job_inactive ~* 'Inactive'
            AND date_lastupdt::date > '2009-12-31'::date
            THEN classa_net
    END AS inactive

FROM final_devdb;
