DROP PROCEDURE IF EXISTS append_to_facdb_base;
CREATE OR REPLACE PROCEDURE append_to_facdb_base(
    _table text
) AS $BODY$
DECLARE
    source text;
BEGIN
    EXECUTE format($n$
        SELECT source FROM %1$I;
    $n$, _table) INTO source;

	EXECUTE format($n$
        DELETE FROM facdb_base
        WHERE source = %1$L;
    $n$, source);

    EXECUTE format($n$
        INSERT INTO facdb_base
        SELECT uid::text,
            source::text,
            facname::text,
            addressnum::text,
            streetname::text,
            address::text,
            city::text,
            zipcode::text,
            boro::text,
            borocode::text,
            bin::text,
            bbl::text,
            factype::text,
            facsubgrp::text,
            opname::text,
            opabbrev::text,
            overabbrev::text,
            capacity::text,
            captype::text,
            wkb_geometry::geometry,
            geo_1b::json,
            geo_bl::json,
            geo_bn::json
        FROM %1$I; $n$, _table);
END
$BODY$
LANGUAGE plpgsql;

DROP TABLE IF EXISTS corrections_applied;
CREATE TABLE corrections_applied (
    uid text,
    field text,
    pre_corr_value text,
    old_value text,
    new_value text
);

DROP TABLE IF EXISTS corrections_not_applied;
CREATE TABLE corrections_not_applied (
    uid text,
    field text,
    pre_corr_value text,
    old_value text,
    new_value text
);

DROP PROCEDURE IF EXISTS correction;
CREATE OR REPLACE PROCEDURE correction(
    _table text,
    _uid text,
    _field text,
    _old_val text,
    _new_val text
) AS $BODY$
DECLARE
    field_type text;
    pre_corr_val text;
    applicable boolean;
BEGIN
    EXECUTE format($n$
        SELECT pg_typeof(a.%1$I) FROM %2$I a LIMIT 1;
    $n$, _field, _table) INTO field_type;

    EXECUTE format($n$
        SELECT a.%1$I::text FROM %2$I a WHERE a.uid = %3$L;
    $n$, _field, _table, _uid) INTO pre_corr_val;

    EXECUTE format($n$
        SELECT %1$L::%3$s = %2$L::%3$s
        OR (%1$L IS NULL AND %2$L IS NULL)
    $n$, pre_corr_val, _old_val, field_type) INTO applicable;

    IF applicable THEN
        RAISE NOTICE 'Applying Correction';
        EXECUTE format($n$
            UPDATE %1$I SET %2$I = %3$L::%4$s WHERE uid = %5$L;
            $n$, _table, _field, _new_val, field_type, _uid);

        EXECUTE format($n$
            DELETE FROM corrections_applied WHERE uid = %1$L AND field = %2$L;
            INSERT INTO corrections_applied VALUES (%1$L, %2$L, %3$L, %4$L, %5L);
            $n$, _uid, _field, pre_corr_val, _old_val, _new_val);
    ELSE
        RAISE NOTICE 'Cannot Apply Correction';
        EXECUTE format($n$
            DELETE FROM corrections_not_applied WHERE uid = %1$L AND field = %2$L;
            INSERT INTO corrections_not_applied VALUES (%1$L, %2$L, %3$L, %4$L, %5L);
            $n$, _uid, _field, pre_corr_val, _old_val, _new_val);
    END IF;

END
$BODY$ LANGUAGE plpgsql;


DROP PROCEDURE IF EXISTS apply_correction;
CREATE OR REPLACE PROCEDURE apply_correction(
    _schema text,
    _table text,
    _corrections text
) AS $BODY$
DECLARE
    _uid text;
    _field text;
    _old_value text;
    _new_value text;
    _valid_fields text[];
BEGIN
    RAISE NOTICE 'Attempting to apply corrections for field % to %.%', _field, _schema, _table;
    SELECT array_agg(column_name) FROM information_schema.columns
    WHERE table_schema = _schema AND table_name = lower(_table) INTO _valid_fields;

    FOR _uid, _field, _old_value, _new_value IN
        EXECUTE FORMAT($n$
            SELECT uid, field, old_value, new_value
            FROM %1$s
            WHERE field = any(%2$L)
        $n$, _corrections, _valid_fields)
    LOOP
        CALL correction(_table, _uid, _field, _old_value, _new_value);
    END LOOP;
END
$BODY$ LANGUAGE plpgsql;


DROP PROCEDURE IF EXISTS replace_empty_strings;
CREATE OR REPLACE PROCEDURE replace_empty_strings(
    _schema text,
    _table text
) AS $BODY$
DECLARE 
    col text;
BEGIN
    -- extract list of columns of VARCHAR type from table
    RAISE NOTICE 'Attempting to replace empty strings ('''' or '' '') with NULL in columns of VARCHAR type in %.% table', _schema, _table;
    FOR col IN (
        SELECT column_name 
        FROM information_schema.columns
        WHERE table_name = _table AND
            table_schema = _schema AND
            data_type = 'character varying'
) 
    LOOP
        EXECUTE FORMAT($n$
            UPDATE %I SET %I = NULLIF(TRIM(%I), '') WHERE %I = '' OR TRIM(%I) = ''; 
            $n$, _table, col, col, col, col);
    END LOOP;
END
$BODY$ LANGUAGE plpgsql;


DROP FUNCTION IF EXISTS clean_charter_school_name;
/*
This is only used for cleaning charter school names from doe_lcgms
and nysed_activeinstitutions at time of deduplication

Any other use should evaluate whether all of these rules are appropriate
*/
CREATE FUNCTION clean_charter_school_name(facname text) RETURNS text AS $$
DECLARE
    _name text := facname;
BEGIN
    _name := replace(_name, '-', ' ');
    _name := replace(_name, '–', ' ');
    _name := replace(_name, '.', '');
    _name := replace(_name, ':', '');
    _name := replace(_name, '&', 'AND');
    _name := regexp_replace(_name, ' CS(?= |$)', ' CHARTER SCHOOL');
    _name := regexp_replace(_name, ' MS(?= |$)', ' MIDDLE SCHOOL');
    _name := regexp_replace(_name, ' HS(?= |$)', ' HIGH SCHOOL');
    _name := regexp_replace(_name, ' NY(?= |$)', ' NEW YORK');
    _name := regexp_replace(_name, '(?= |^)NYC(?= |$)', 'NEW YORK CITY');
    _name := regexp_replace(_name, ' PREP(?= |$)', ' PREPARATORY');
    _name := regexp_replace(_name, ' (II|LL)(?= |$)', ' 2');
    _name := regexp_replace(_name, ' LLL(?= |$)', ' III');
    -- very specific case. one record has 4 as "lV" - lowercase L, uppercase V
    -- FacDB cleans to all caps by default
    _name := regexp_replace(_name, ' (LV)(?= |$)', ' IV');
    _name := regexp_replace(_name, '(?= |^)THE ', '', 'g');
    _name := regexp_replace(_name, '\(.*\)', '', 'g');
    _name := regexp_replace(_name, '\s+', ' ', 'g');
    _name := regexp_replace(_name, '\s+$', '');
    _name := regexp_replace(_name, '^\s+', '');
    return _name;
END
$$ LANGUAGE plpgsql;
