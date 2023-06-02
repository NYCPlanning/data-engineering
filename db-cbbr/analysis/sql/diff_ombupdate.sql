-- what are the differences between the OMB update and the original CBBR submissions

-- records in cbbr and not in omb data
COPY(
	SELECT * FROM cbbr_submissions a
	LEFT JOIN (
		SELECT a.*, regid as regidb
		FROM cbbr_ombresponse a
		LEFT JOIN cbbr_omblookuptable b
		ON a.newtrackingno=b.newtrackingno
	) b
ON upper(a.regid)=b.regidb
WHERE b.regidb IS NULL
) TO '/prod/db-cbbr/analysis/output/cbbr_records_notinomb.csv' DELIMITER ',' CSV HEADER;

-- records in omb data and not in original cbbr data
WITH joined as (
	SELECT * FROM cbbr_ombresponse a
	LEFT JOIN (
		SELECT a.*, b.regid as regidb
		FROM cbbr_submissions a
		LEFT JOIN cbbr_omblookuptable b
		ON upper(a.regid)=b.regid 
	) b
ON a.newtrackingno=b.newtrackingno
)
SELECT * FROM joined
WHERE regidb IS NULL;