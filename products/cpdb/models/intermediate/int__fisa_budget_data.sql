SELECT
    LPAD(mng_dpt_cd, 3, '0') || cptl_proj_id AS maprojid,
    LPAD(mng_dpt_cd, 3, '0') AS magency,
    cptl_proj_id AS projectid,
    rcls_cd AS occurance,
    atyp_cd AS fundingsource,
    bud_obj_cd AS commitmentcode,
    au_cd AS appropriationunit,
    cmtmnt_am,
    oblgtns_am,
    adpt_am,
    penc_am,
    enc_am,
    acrd_exp_am,
    cash_exp_am,
    ucomit_am,
    actu_exp_am
FROM {{ ref('stg__fisa_dailybudget') }}
