DO
$$
DECLARE
BEGIN
	INSERT INTO lfl_test.mv_refresh_01_tbl VALUES (4, 'eeeed');

	REFRESH MATERIALIZED VIEW CONCURRENTLY lfl_test.mv_refresh_02_mv;

	--sleep not necessary if in "manual commit" mode
	--PERFORM pg_sleep(10);
END;
$$
;