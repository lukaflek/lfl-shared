-- Use case definition
--	- A query runs a long time
--	- AND the explain plan has many rows
-- What is the struggle
--	- "explain" returns/fetches first n-rows (according to ide setting) after it has been running for quite a long time
--	- Once you need to fetch another set of rows, you need to wait as long as the last time. That might be very frustrating
--	- The solution below makes it possible to save the whole execution plan into a table  

--================
-- 1. Function that returns the ep (necessary because it is not possible to save the ep in a table directly)
--================
DROP FUNCTION IF EXISTS lfl_test.get_explain_plan;

CREATE OR REPLACE FUNCTION lfl_test.get_explain_plan(
	it_sql TEXT
)
	RETURNS TABLE (ep_row TEXT)
	LANGUAGE plpgsql
AS
$$
DECLARE
	--Adds explain "configuration" to the provided query
	vt_sql	TEXT := 'explain (analyze ON, VERBOSE ON, BUFFERS ON, costs ON, wal ON, timing ON, SUMMARY on) ' || it_sql;
BEGIN
	RAISE NOTICE '%', vt_sql;
	RETURN query 
		EXECUTE vt_sql;
END
$$
;

--================
-- 2. Ccreates the table to save the ep into
--================
DROP TABLE IF EXISTS lfl_test.explain_plain;

CREATE TABLE lfl_test.explain_plain(
	ins_dt	timestamp WITH time zone,
	ep_row	TEXT
)
;

TRUNCATE TABLE lfl_test.explain_plain;

--================
-- 3. Saves the ep into a table
--================
INSERT INTO lfl_test.explain_plain
SELECT
	clock_timestamp() AS ins_dt,
	--
	--aggregation of all rows into a single one
	--string_agg(ep_row, E'\n')
	--
	--every row as a separate one
	*
FROM
	--Escaping single quotes: E'\'aaa\'' = 'aaa'
	lfl_test.get_explain_plan(E'SELECT
	*
FROM
	fg_01798570.organizational_structure
WHERE 1 = 1
	AND parent_organization_id LIKE concat(\'%\', \'11198570\', \'%\') 
	AND validity=TRUE')
;

--================
-- 4. Gets the ep from the table
--================
SELECT *
FROM
	lfl_test.explain_plain
;