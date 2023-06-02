-- transform I/R RPAD codes to Y/N codes for irregular lot codes
UPDATE pluto
SET irrlotcode = 
	(CASE 
		WHEN irrlotcode = 'I' THEN 'Y'
		ELSE 'N'
	END);