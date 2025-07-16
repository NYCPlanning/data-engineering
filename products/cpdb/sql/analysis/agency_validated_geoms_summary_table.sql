-- create summary table reporting how many projects and total $ of planned commitments agencies added to the map or corrected
DROP TABLE IF EXISTS agency_validated_geoms_summary_table;
CREATE TABLE agency_validated_geoms_summary_table AS (
    WITH newgeoms AS (
        SELECT
            a.agency,
            count(a.*) AS countnew,
            sum(b.plannedcommit_total) AS totalplannedcommitnew
        FROM dcp_cpdb_agencyverified AS a
        LEFT JOIN ccp_projects AS b
            ON a.maprojid = b.maprojid
        WHERE
            origin = 'unmapped'
            AND a.geom IS NOT NULL
        GROUP BY agency
    ),

    correctedgeoms AS (
        SELECT
            a.agency,
            count(a.*) AS countcorrected,
            sum(b.plannedcommit_total) AS totalplannedcommitcorrected
        FROM dcp_cpdb_agencyverified AS a
        LEFT JOIN ccp_projects AS b
            ON a.maprojid = b.maprojid
        WHERE
            origin = 'mapped'
            AND a.geom IS NOT NULL
        GROUP BY agency
    ),

    removedgeoms AS (
        SELECT
            a.agency,
            count(a.*) AS countremoved,
            sum(b.plannedcommit_total) AS totalplannedcommitremoved
        FROM dcp_cpdb_agencyverified AS a
        LEFT JOIN ccp_projects AS b
            ON a.maprojid = b.maprojid
        WHERE
            origin = 'mapped'
            AND (
                mappable = 'No - Can be in future'
                OR mappable = 'No - Can never be mapped'
            )
        GROUP BY agency
    ),

    totalmappedrecords AS (
        SELECT
            a.agency,
            count(a.*) AS totalmapped
        FROM dcp_cpdb_agencyverified AS a
        WHERE origin = 'mapped'
        GROUP BY agency
    ),

    totalunmappedrecords AS (
        SELECT
            a.agency,
            count(a.*) AS totalunmapped
        FROM dcp_cpdb_agencyverified AS a
        WHERE origin = 'unmapped'
        GROUP BY agency
    )

    SELECT
        a.agency,
        totalmapped,
        totalunmapped,
        countcorrected,
        totalplannedcommitcorrected,
        countnew,
        totalplannedcommitnew,
        countremoved,
        totalplannedcommitremoved,
        (countcorrected + countnew) - countremoved AS countnetchange,
        (totalplannedcommitcorrected + totalplannedcommitnew) - totalplannedcommitremoved AS totalplannedcommitnetchange
    FROM totalunmappedrecords AS a
    LEFT JOIN totalmappedrecords AS b
        ON a.agency = b.agency
    LEFT JOIN correctedgeoms AS c
        ON a.agency = c.agency
    LEFT JOIN newgeoms AS d
        ON a.agency = d.agency
    LEFT JOIN removedgeoms AS e
        ON a.agency = e.agency
    ORDER BY a.agency
);
