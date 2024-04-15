--================
-- an exception implicitly rolls back
-- - EXCEPTION raises an error (which normally aborts the current transaction) (https://www.postgresql.org/docs/13/plpgsql-errors-and-messages.html)
-- - ABORT rolls back the current transaction and causes all the updates made by the transaction to be discarded (https://www.postgresql.org/docs/current/sql-abort.html)
--================

-- table
TRUNCATE TABLE lfl_test.test_tbl;

DROP TABLE IF EXISTS lfl_test.test_tbl;

CREATE TABLE IF NOT EXISTS lfl_test.test_tbl(
	val TEXT
);

SELECT * FROM lfl_test.test_tbl;

INSERT INTO lfl_test.test_tbl VALUES ('a');

-- procedure (inserts a record to the table)
CREATE OR REPLACE PROCEDURE lfl_test.ins_test_tbl(
	it_val TEXT
)
	LANGUAGE plpgsql
AS
$$
BEGIN
	INSERT INTO lfl_test.test_tbl (val) VALUES (it_val);
END;
$$
;

-- using the procedure in an anonymous block, which raises an exception
DO
$$
DECLARE
	vi_val int;
BEGIN
	CALL
		lfl_test.ins_test_tbl(
			to_char(clock_timestamp(), 'yyyymmddhh_hh24miss')
		);

	RAISE NOTICE 'inserted';

	--if we want to prevent the possible exception raised by the select from rolling back the previous DML, we need to use separate begin-exception (!!)-end block (just begin-end is not sufficient!!) 
	BEGIN
		SELECT 1/0 INTO vi_val;
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'exception 01';
	END;
/*EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE 'exception 02';
		--COMMIT;
		--ROLLBACK;*/
END;
$$
;