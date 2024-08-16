DO
$$
DECLARE
	vt_val	TEXT;
BEGIN
	--All cases succeed
	RAISE NOTICE '--== Without STRICT';
	--
	RAISE NOTICE '1 row returned';
	WITH
		cte_dataset AS (
			SELECT * FROM generate_series (0, 10) AS seq (val)
		)
	SELECT
		val::TEXT
	INTO
		vt_val
	FROM
		cte_dataset
	WHERE val = 1;
	RAISE NOTICE 'vt_val: %', vt_val;
	--
	RAISE NOTICE '2+ rows returned';
	WITH
		cte_dataset AS (
			SELECT * FROM generate_series (0, 10) AS seq (val)
		)
	SELECT
		val::TEXT
	INTO
		vt_val
	FROM
		cte_dataset
	WHERE val > 1;
	RAISE NOTICE 'vt_val: %', vt_val;
	--
	RAISE NOTICE 'No row returned';
	WITH
		cte_dataset AS (
			SELECT * FROM generate_series (0, 10) AS seq (val)
		)
	SELECT
		val::TEXT
	INTO
		vt_val
	FROM
		cte_dataset
	WHERE val < 0;
	RAISE NOTICE 'vt_val: %', vt_val;

	--------------------------------------------------

	-- - 1 row returned: succeed
	-- - other: fail
	RAISE NOTICE '--== With STRICT';
	--
	RAISE NOTICE '1 row returned';
	WITH
		cte_dataset AS (
			SELECT * FROM generate_series (0, 10) AS seq (val)
		)
	SELECT
		val::TEXT
	INTO STRICT
		vt_val
	FROM
		cte_dataset
	WHERE val = 1;
	RAISE NOTICE 'vt_val: %', vt_val;
	--
	RAISE NOTICE '2+ rows returned';
	WITH
		cte_dataset AS (
			SELECT * FROM generate_series (0, 10) AS seq (val)
		)
	SELECT
		val::TEXT
	INTO STRICT
		vt_val
	FROM
		cte_dataset
	WHERE val > 1;
	RAISE NOTICE 'vt_val: %', vt_val;
	--
	RAISE NOTICE 'No row returned';
	WITH
		cte_dataset AS (
			SELECT * FROM generate_series (0, 10) AS seq (val)
		)
	SELECT
		val::TEXT
	INTO STRICT
		vt_val
	FROM
		cte_dataset
	WHERE val < 0;
	RAISE NOTICE 'vt_val: %', vt_val;
END;
$$
;