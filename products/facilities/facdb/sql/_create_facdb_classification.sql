DROP TABLE IF EXISTS facdb_classification;
SELECT
    a.uid,
    a.facsubgrp,
    b.facgroup,
    b.facdomain,
    b.servarea
INTO facdb_classification
FROM facdb_base AS a
INNER JOIN lookup_classification AS b
    ON upper(a.facsubgrp) = upper(b.facsubgrp);
