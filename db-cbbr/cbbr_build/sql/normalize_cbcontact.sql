-- Append agency contact info onto each record

-- create table of agency contact info
DROP TABLE IF EXISTS cbbr_cbcontacts;
CREATE TABLE cbbr_cbcontacts (
parentregid text,
fullboardname text,
first text,
last text,
title text,
email text,
phone text
);

COPY cbbr_cbcontacts FROM '/prod/db-cbbr/cbbr_build/cbbr_cbcontacts.csv' DELIMITER ',' CSV;

-- append agency contact info
UPDATE cbbr_submissions
SET cifirst = b.first,
	cilast = b.last,
	cititle = b.title,
	ciemail = b.email,
	ciphone = b.phone
FROM cbbr_cbcontacts b
WHERE name = b.fullboardname
;