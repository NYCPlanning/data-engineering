SELECT
    cmplnt_num AS complaint_id,
    cmplnt_fr_dt::timestamp::date AS occurred_date,
    rpt_dt::timestamp::date AS reported_date,
    ky_cd::integer AS offense_code,
    ofns_desc AS offense_description,
    pd_desc AS internal_offense_description,
    law_cat_cd AS law_category,
    crm_atpt_cptd_cd = 'ATTEMPTED' AS is_attempted,
    addr_pct_cd::integer AS precinct,
    boro_nm AS boro_name,
    prem_typ_desc AS premises_type,
    juris_desc AS jurisdiction,
    geom
FROM {{ source('sources', 'nypd_complaints_historic') }}
