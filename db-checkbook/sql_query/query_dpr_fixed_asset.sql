UPDATE capital_projects SET bc_category = 'Fixed Asset'
WHERE [Budget Code] REGEXP '[BMQRX][0-9][0-9][0-9]' AND [Agency] = 'Department of Parks and Recreation'
AND bc_category IS NULL;