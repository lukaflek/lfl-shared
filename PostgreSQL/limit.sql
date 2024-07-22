WITH
	cte_dataset AS (
		SELECT * FROM generate_series (0, 10) AS seq (val)
	),
	--steps (defined by sql standard)
	--	1. aggregates all rows, which meet the where condition (returns 1 row)
	--	2. applies LIMIT (return 4 rows), which actually returns the only (agregated) one (because there are no other ones)
	cte_limit_wrong AS (
		SELECT
			JSONB_AGG(val)
		FROM
			cte_dataset
		WHERE val < 8
		LIMIT
			4
	),
	--steps (defined manually)
	--	1. applied LIMIT to the basic dataset (returns 4 rows)
	--	2. aggregates the limited output
	cte_limit_correct_step_1 AS (
		SELECT
			*
		FROM
			cte_dataset
		LIMIT
			4
	),
	cte_limit_correct_step_2 AS (
		SELECT
			JSONB_AGG(val)
		FROM
			cte_limit_correct_step_1
	)
SELECT
	*
FROM
	--cte_limit_wrong
	--
	--cte_limit_correct_step_1
	cte_limit_correct_step_2
WHERE TRUE
;