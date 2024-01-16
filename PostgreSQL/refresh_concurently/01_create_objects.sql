DROP TABLE IF EXISTS lfl_test.mv_refresh_01_tbl;
CREATE TABLE IF NOT EXISTS lfl_test.mv_refresh_01_tbl
(
	id		SMALLINT,
	val_1	varchar
);

INSERT INTO lfl_test.mv_refresh_01_tbl VALUES (1, 'aa');
INSERT INTO lfl_test.mv_refresh_01_tbl VALUES (2, 'bb');

SELECT * FROM lfl_test.mv_refresh_01_tbl;

--================

DROP MATERIALIZED VIEW IF EXISTS lfl_test.mv_refresh_02_mv;
CREATE MATERIALIZED VIEW IF NOT EXISTS lfl_test.mv_refresh_02_mv
AS
SELECT 1 AS id, clock_timestamp() AS dt 
;

--prerequisite for REFRESH ... CONCURRENTLY
CREATE UNIQUE INDEX ON lfl_test.mv_refresh_02_mv (id);

SELECT * FROM lfl_test.mv_refresh_02_mv;
	