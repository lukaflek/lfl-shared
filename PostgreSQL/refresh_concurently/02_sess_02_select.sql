--takeaways
--	- assumption: the "main" dml transaction has not been comitted yet
--	- tbl
--		- tbl is NOT locked (is reachable)
--		- new rec NOT visible (until the main transaction is committed) 
--	- mv
--		- refresh WITHOUT concurently
--			- mv is locked (not reachable until the main transaction is committed)
--		- refresh WITH concurently
--			- mv is NOT locked (is reachable)
--			- exposes data valid before the refresh
--			- new data are exposed after the main transaction is committed
SELECT * FROM lfl_test.mv_refresh_01_tbl;

SELECT * FROM lfl_test.mv_refresh_02_mv;