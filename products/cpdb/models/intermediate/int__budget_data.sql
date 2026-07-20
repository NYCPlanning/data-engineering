SELECT
    b.maprojid,
    b.magency,
    b.projectid,
    b.fundingsource,
    SUM(b.cmtmnt_am) AS cmtmnt_am,
    SUM(b.oblgtns_am) AS oblgtns_am,
    SUM(b.adpt_am) AS adpt_am,
    SUM(b.penc_am) AS penc_am,
    SUM(b.enc_am) AS enc_am,
    SUM(b.acrd_exp_am) AS acrd_exp_am,
    SUM(b.cash_exp_am) AS cash_exp_am,
    SUM(b.ucomit_am) AS ucomit_am,
    SUM(b.actu_exp_am) AS actu_exp_am
FROM {{ ref('int__ccp_projects') }} AS a
INNER JOIN {{ ref('int__fisa_budget_data') }} AS b ON a.maprojid = b.maprojid
GROUP BY b.magency, b.projectid, b.maprojid, b.fundingsource
