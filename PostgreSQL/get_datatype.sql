--------------------------------------------------
-- - pg_typeof: doesn't recognize various json datatypes
-- - json datatypes are recognized by JSONB_TYPEOF
-- - combination of pg_typeof/JSONB_TYPEOF is omplemented in a method
--------------------------------------------------

--recognizes various json datatypes as well
DROP FUNCTION lfl_test.work_w_anyelement;

CREATE OR REPLACE FUNCTION lfl_test.work_w_anyelement(
	ix_any IN anyelement
)
 	RETURNS TEXT
	LANGUAGE plpgsql
AS
$$
DECLARE
	vt_datatype		TEXT;
	vt_datatype_js	TEXT;
	vt_val			TEXT;
	vt_return		TEXT;
BEGIN
	--gets the basic datatype. returns 'jsonb' for any json value
	vt_datatype := pg_typeof(ix_any);
	
	IF vt_datatype = 'jsonb' THEN
		--gets the json value datatype
		vt_datatype_js := JSONB_TYPEOF(ix_any);
		vt_datatype := vt_datatype || '.' || vt_datatype_js;
	END IF;
	
	--implicit text conversion
	--vt_val := ix_any::TEXT;
	
	--vt_return := format('%s (%s)', vt_datatype, vt_val);
	vt_return := vt_datatype;
	
	RETURN vt_return;
END;
$$
;

--usage
WITH
	--prepares various values/datatypes (incl. json)
	cte_data AS (
		SELECT
			'ahoj'::TEXT					AS val_text
			,'ahoj'::VARCHAR				AS val_varchar
			--
			,1::INT							AS val_int
			,1.123::NUMERIC					AS val_numeric
			--
			,clock_timestamp()::DATE		AS val_date
			,clock_timestamp()::TIMESTAMP	AS val_timestamp
			--
			,NULL							AS val_null
			,JSONB_BUILD_OBJECT(
				'text', 'ahoj'
				,'int', 1
				,'numeric', 1.123
				,'null', NULL
			)								AS val_jsonb
	)
SELECT
	--==scalar/simple datatypes: pg_typeof is good enough
	pg_typeof(c.val_text)		AS val_text
	,pg_typeof(c.val_varchar)	AS val_varchar
	,pg_typeof(c.val_int)		AS val_int
	,pg_typeof(c.val_numeric)	AS val_numeric
	,pg_typeof(c.val_date)		AS val_date
	,pg_typeof(c.val_timestamp)	AS val_text
	--returns always "text"
	,pg_typeof(c.val_null)		AS val_null
	--more versatile
	,lfl_test.work_w_anyelement(NULL::TEXT)	AS val_null_tx	
	,lfl_test.work_w_anyelement(NULL::INT)	AS val_null_int
	--== json
	,pg_typeof(c.val_jsonb)					AS val_jsonb
	--pg_typeof doesn't recognize various json datatypes (returns "jsonb")
	,pg_typeof(c.val_jsonb -> 'text')		AS val_jsonb_text
	,pg_typeof(c.val_jsonb -> 'int')		AS val_jsonb_int
	--recognizes various json datatypes as well (the function id defined below)
	,lfl_test.work_w_anyelement(c.val_jsonb -> 'text')		AS any_json_text
	,lfl_test.work_w_anyelement(c.val_jsonb -> 'int')		AS any_json_int
	,lfl_test.work_w_anyelement(c.val_jsonb -> 'numeric')	AS any_json_numeric
	,lfl_test.work_w_anyelement(c.val_jsonb -> 'null')		AS any_json_null
FROM
	cte_data AS c
;