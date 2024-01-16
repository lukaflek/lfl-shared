--takeaways (
--	- assumption: the dml transaction is not comitted
--	- refresh mv WITHOUT concurently
--		- tbl: tbl is not locked (is reachable); new rec not visible
--		- mv: mv is locked (not reachable); gets reachable after commit
--	- refresh mv WITH concurently
--		- tbl: tbl is not locked (is reachable); new rec not visible
--		- mv: mv is not locked (is reachable); new recs are not visible (mv provides data valid before refresh), new data are provided after commit
SELECT * FROM lfl_test.mv_refresh_01_tbl;

SELECT * FROM lfl_test.mv_refresh_02_mv;