DROP TABLE IF EXISTS modifications_applied;
CREATE TABLE modifications_applied (
    uid text,
    field text,
    old_value text,
    new_value text
);

DROP TABLE IF EXISTS modifications_not_applied;
CREATE TABLE modifications_not_applied (
    uid text,
    field text,
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
    records_to_recode boolean;
BEGIN
    EXECUTE format($n$
        SELECT pg_typeof(a.%1$I) FROM %2$I a LIMIT 1;
    $n$, _field, _table) INTO field_type;

    IF _uid IS NOT NULL THEN
        EXECUTE format($n$
            SELECT count(*) >0 FROM %1$I a WHERE a.uid = %2$L;
        $n$, _table, _uid) INTO records_to_recode;
    ELSE
        EXECUTE format($n$
            SELECT count(*)>0 FROM %2$I a WHERE a.%1$I::text = %3$L;
        $n$, _field, _table, _old_val) INTO records_to_recode;
    END IF;

    IF records_to_recode THEN 
        RAISE NOTICE 'Applying Correction';
        IF _uid IS NOT NULL THEN
            EXECUTE format($n$
                UPDATE %1$I SET %2$I = %4$L::%5$s, "DCPEDITED" = 'Y' WHERE %1$I.uid = %3$L;
            $n$, _table, _field, _uid, _new_val, field_type);
            EXECUTE format($n$
                DELETE FROM modifications_applied WHERE uid = %1$L AND field = %2$L AND old_value = %3$L;
            $n$, _uid, _field, _old_val, _new_val);
        ELSE
            EXECUTE format($n$
                UPDATE %1$I SET %2$I = %4$L::%5$s, "DCPEDITED" = 'Y' WHERE %2$I = %3$L;
                $n$, _table, _field, _old_val, _new_val, field_type);
        END IF;
        EXECUTE format($n$
            INSERT INTO modifications_applied VALUES (%1$L, %2$L, %3$L, %4L);
            $n$, _uid, _field, _old_val, _new_val);
    ELSE 
        RAISE NOTICE 'Cannot Apply Correction to field % in table % of changing value % to % ',
            _field, _table, _old_val, _new_val;
        EXECUTE format($n$
            DELETE FROM modifications_not_applied WHERE uid = %1$L AND field = %2$L;
            INSERT INTO modifications_not_applied VALUES (%1$L, %2$L, %3$L, %4L);
            $n$, _uid, _field, _old_val, _new_val);
    END IF;

END
$BODY$ LANGUAGE plpgsql;


DROP PROCEDURE IF EXISTS apply_correction;
CREATE OR REPLACE PROCEDURE apply_correction(
    _table text,
    _schema text,
    _modifications text
) AS $BODY$
DECLARE 
    _uid text;
    _field text;
    _old_value text;
    _new_value text;
    _valid_fields text[];
BEGIN
    SELECT array_agg(column_name) FROM information_schema.columns
    WHERE table_schema = _schema AND table_name = _table INTO _valid_fields;

    FOR _uid, _field, _old_value, _new_value IN 
        EXECUTE FORMAT($n$
            SELECT uid, field, old_value, new_value 
            FROM %1$s
            WHERE field = any(%2$L)
        $n$, _modifications, _valid_fields)
    LOOP
        CALL correction(_table, _uid, _field, _old_value, _new_value);
    END LOOP;
END
$BODY$ LANGUAGE plpgsql;
