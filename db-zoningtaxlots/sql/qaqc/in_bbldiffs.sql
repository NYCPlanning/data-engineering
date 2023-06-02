-- output a diff file with bbls that have changed in any field
CREATE TEMP TABLE tmp (
    boroughcode text,
    taxblock text,
    taxlot text,
    bblnew text,
    zd1new text,
    zd2new text,
    zd3new text,
    zd4new text,
    co1new text,
    co2new text,
    sd1new text,
    sd2new text,
    sd3new text,
    lhdnew text,
    zmnnew text,
    zmcnew text,
    area text,
    inzonechange text,
    bblprev text,
    zd1prev text,
    zd2prev text,
    zd3prev text,
    zd4prev text,
    co1prev text,
    co2prev text,
    sd1prev text,
    sd2prev text,
    sd3prev text,
    lhdprev text,
    zmnprev text,
    zmcprev text
);

\COPY tmp FROM PSTDIN DELIMITER ',' CSV HEADER;

DROP TABLE IF EXISTS qc_bbldiffs;
SELECT a.*, b.geom
INTO qc_bbldiffs
FROM tmp a
JOIN dof_dtm b
on a.bblnew::text = b.bbl::text;