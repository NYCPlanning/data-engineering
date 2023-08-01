DROP TABLE IF EXISTS qaqc_field_distribution;
CREATE TABLE IF NOT EXISTS qaqc_field_distribution(
    field_name character varying,
    result character varying
);

INSERT INTO qaqc_field_distribution (
    SELECT 'Job_Type' as field_name,
            jsonb_agg(json_build_object('job_type',tmp.job_type,'count',tmp.count)) as result
    FROM(
        SELECT job_type, COUNT(DISTINCT job_number) as count
        FROM final_devdb
        WHERE is_date(date_lastupdt) AND 
              date_lastupdt::date > :'CAPTURE_DATE_PREV'::date 
        GROUP BY job_type) tmp );

INSERT INTO qaqc_field_distribution(
    SELECT 'Job_Status' as field_name,
            jsonb_agg(json_build_object('job_status',tmp.job_status,'count',tmp.count)) as result 
    FROM(
        SELECT job_status, COUNT(DISTINCT job_number) as count
        FROM final_devdb
        WHERE is_date(date_lastupdt) AND 
              date_lastupdt::date > :'CAPTURE_DATE_PREV'::date 
        GROUP BY job_status) tmp );