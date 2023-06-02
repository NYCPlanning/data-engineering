UPDATE pluto_rpad_geo
SET billingblock = SUBSTRING(billingbbl, 2, 5),
	billinglot = RIGHT(billingbbl, 4)
WHERE billingbbl IS NOT NULL 
	AND billingbbl <> '0000000000' 
	AND billingbbl <> 'none';