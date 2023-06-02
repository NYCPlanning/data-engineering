DROP TABLE IF EXISTS facdb_classification;
SELECT
    a.uid,
    a.facsubgrp,
    b.facgroup,
    b.facdomain,
    b.servarea
INTO facdb_classification
FROM facdb_base a
JOIN lookup_classification b
ON UPPER(a.facsubgrp) = UPPER(b.facsubgrp);
