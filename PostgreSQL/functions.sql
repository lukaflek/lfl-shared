--================
-- define
--================

DROP PROCEDURE IF EXISTS lfl_test.f_test();

CREATE OR REPLACE FUNCTION lfl_test.f_test(
	in_val	IN 		NUMERIC
)
	RETURNS NUMERIC
	LANGUAGE plpgsql
AS
$$
DECLARE
	vn_return	NUMERIC;
BEGIN
	--not possible
	--f_test := in_val * 100;
	
	--option 1
	--RETURN in_val * 100;
	
	--option 2
	vn_return := in_val * 100;
	RETURN vn_return;
END;
$$
;

--================
-- call
--================

DO
$$
DECLARE
	vn_val	NUMERIC;
BEGIN
	vn_val :=
		lfl_test.f_test(
			in_val	=> 2);
	RAISE NOTICE 'vn_val: %', vn_val;
END;
$$
;

--================
-- immutable, stable
--================
CREATE OR REPLACE FUNCTION lfl_test.f_ordinary()
	RETURNS TEXT[]
	LANGUAGE plpgsql
AS
$$
BEGIN
	RETURN ARRAY['CommunityPartyRoleGroup', 'FunWorkgroupPartyRoleGroup']::TEXT[];
END;
$$
;

CREATE OR REPLACE FUNCTION lfl_test.f_immutable()
	RETURNS TEXT[]
	LANGUAGE plpgsql
	IMMUTABLE
AS
$$
BEGIN
	RETURN ARRAY['CommunityPartyRoleGroup', 'FunWorkgroupPartyRoleGroup']::TEXT[];
END;
$$
;

CREATE OR REPLACE FUNCTION lfl_test.f_stable()
	RETURNS TEXT[]
	LANGUAGE plpgsql
	STABLE
AS
$$
BEGIN
	RETURN ARRAY['CommunityPartyRoleGroup', 'FunWorkgroupPartyRoleGroup']::TEXT[];
END;
$$
;

SELECT lfl_test.f_ordinary();
SELECT lfl_test.f_immutable();
SELECT lfl_test.f_stable();

DO
$$
DECLARE
	ci_loop_cnt	constant	int := 100000;
	vt_start	timestamp;
	vt_end		timestamp;
	vt_dur		interval;
BEGIN
	RAISE NOTICE 'ci_loop_cnt: %', ci_loop_cnt::TEXT;
	RAISE NOTICE '';

	RAISE NOTICE '---- f_ordinary ----';
	vt_start := clock_timestamp()::timestamp(3);
	RAISE NOTICE 'START: %', vt_start;
	FOR i IN 1 .. ci_loop_cnt
	LOOP
		PERFORM
			lfl_test.f_ordinary();
	END LOOP;
	vt_end := clock_timestamp()::timestamp(3);
	RAISE NOTICE 'END: %', vt_end;

	vt_dur := vt_end - vt_start;
	RAISE NOTICE 'DURATION: %', vt_dur;
	RAISE NOTICE '';

	RAISE NOTICE '---- f_immutable ----';
	vt_start := clock_timestamp()::timestamp(3);
	RAISE NOTICE 'START: %', vt_start;
	FOR i IN 1 .. ci_loop_cnt
	LOOP
		PERFORM
			lfl_test.f_immutable();
	END LOOP;
	vt_end := clock_timestamp()::timestamp(3);
	RAISE NOTICE 'END: %', vt_end;

	vt_dur := vt_end - vt_start;
	RAISE NOTICE 'DURATION: %', vt_dur;
	RAISE NOTICE '';

	RAISE NOTICE '---- f_stable ----';
	vt_start := clock_timestamp()::timestamp(3);
	RAISE NOTICE 'START: %', vt_start;
	FOR i IN 1 .. ci_loop_cnt
	LOOP
		PERFORM
			lfl_test.f_stable();
	END LOOP;
	vt_end := clock_timestamp()::timestamp(3);
	RAISE NOTICE 'END: %', vt_end;

	vt_dur := vt_end - vt_start;
	RAISE NOTICE 'DURATION: %', vt_dur;
END;
$$
;
	