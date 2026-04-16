UPDATE _cbbr_submissions a
SET
    policy_area = 'Other Needs',
    need_group = 'Other',
    need = 'Other Capital Needs'
WHERE
    need IS NULL
    AND agency_acronym = 'DSNY'
    AND type = 'Capital';
