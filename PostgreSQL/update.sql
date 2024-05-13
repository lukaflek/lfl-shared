DROP TABLE IF EXISTS lfl_test.tbl_test;

CREATE TABLE lfl_test.tbl_test(
	id			INT,
	col_txt		TEXT,
	col_uuid	UUID
);

INSERT INTO lfl_test.tbl_test VALUES (1, 'aa', public.gen_random_uuid());
INSERT INTO lfl_test.tbl_test VALUES (2, 'bb', public.gen_random_uuid());

SELECT * FROM lfl_test.tbl_test;

--================
-- update multiple rows and return them: it is not straight-forward since it is not possible to use aggregate functions in the returning clause
--================

DO
$$
DECLARE
	vj_updated	JSONB;
BEGIN
	--== single record: works fine
	/*UPDATE
		lfl_test.tbl_test AS tt
	SET
		col_txt = col_txt || '|upd'
	WHERE tt.id = 1
	RETURNING
		to_jsonb(tt)
	INTO STRICT
		vj_updated;*/

	--== multiple reords
	-- straight-forward approach: doesn't work 
	/*UPDATE
		lfl_test.tbl_test AS tt
	SET
		col_txt = col_txt || '|upd'
	WHERE tt.id < 10
	--It is not possible to use eg jsonb_agg (ERROR: aggregate functions are not allowed in RETURNING) 
	RETURNING
		jsonb_agg(
			to_jsonb(tt)
		)
	INTO STRICT
		vj_updated;*/
	
	--more complicated: works (https://www.postgresql.org/docs/13/queries-with.html#QUERIES-WITH-MODIFYING)
	WITH
		cte_update AS (
			UPDATE
				lfl_test.tbl_test AS tt
			SET
				col_txt = col_txt || '|upd'
			WHERE tt.id < 10
			RETURNING *
		)
	SELECT
		jsonb_agg(
			to_jsonb(cte_update)
		)
	INTO STRICT
		vj_updated
	FROM
		cte_update;
	
	RAISE NOTICE 'vj_updated: %', jsonb_pretty(vj_updated);
END;
$$
;