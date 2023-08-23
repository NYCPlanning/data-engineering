-- getting distinct BBLs FROM raw data
ALTER TABLE pluto_input_geocodes
RENAME bbl to geo_bbl;

UPDATE pluto_input_geocodes
SET xcoord = ST_X(ST_TRANSFORM(geom, 2263))::integer,
    ycoord = ST_Y(ST_TRANSFORM(geom, 2263))::integer,
    ct2010 = (CASE WHEN ct2010::numeric = 0 THEN NULL ELSE ct2010 END);

DROP TABLE IF EXISTS pluto_rpad_geo;
CREATE TABLE pluto_rpad_geo AS (
WITH pluto_rpad_rownum AS (
	SELECT a.*, ROW_NUMBER()
    	OVER (PARTITION BY boro||tb||tl
      	ORDER BY curavt_act DESC, land_area DESC, ease ASC) AS row_number
  		FROM dof_pts_propmaster a),
pluto_rpad_sub AS (
	SELECT * 
	FROM pluto_rpad_rownum 
	WHERE row_number = 1)
SELECT a.*, b.*
FROM pluto_rpad_sub a
LEFT JOIN pluto_input_geocodes b
ON a.boro||a.tb||a.tl=b.borough||lpad(b.block,5,'0')||lpad(b.lot,4,'0')
);



ALTER TABLE pluto_rpad_geo RENAME numberofexistingstructures TO numberOfExistingStructuresOnLot;

ALTER TABLE pluto_rpad_geo ADD ap_datef text;

UPDATE pluto_rpad_geo SET bbl = borough||lpad(block,5,'0')||lpad(lot,4,'0');

-- backfill X and Y coordinates
UPDATE pluto_rpad_geo a SET 
	xcoord = ST_X(ST_TRANSFORM(b.geom, 2263))::integer
	,ycoord = ST_Y(ST_TRANSFORM(b.geom, 2263))::integer
FROM (
	SELECT 
		a.bbl,
		ST_SetSRID(ST_MakePoint(a.longitude::double precision, a.latitude::double precision),4326) as geom
	FROM pluto_rpad_geo a
	WHERE a.longitude IS NOT NULL
) b
WHERE a.bbl = b.bbl;