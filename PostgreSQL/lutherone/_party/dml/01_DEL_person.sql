DO
$$
DECLARE
	ct_tenant_id constant	TEXT := 'k_2019418';
	cb_only_invalid			BOOLEAN := FALSE;
	vi_rows_affected		INT;
	--
	vt_hostname		TEXT;
	--
	vi_cnt_pers_rl	INT;
	vi_cnt_pers_pa	INT;
	--vi_cnt_group	INT;
	--
	vj_persons			JSONB;
	--vj_pers_pa		JSONB;
	--vj_group		JSONB;
	--
	va_role_id_pers	UUID[];
	va_party_id		UUID[];
	--va_group_id		UUID[];
BEGIN
	RAISE NOTICE 'START: %', common.f_ts_to_char(clock_timestamp()::timestamp);
	RAISE NOTICE '';

	vt_hostname :=
		common_party.f_get_host_param(
			it_key 		=> 'hostname'
			,ix_value	=> NULL::TEXT
		);

	IF lower(vt_hostname) NOT LIKE '%lukas%' THEN
		RAISE 'ACTION REJECTED - hostname: %', vt_hostname;
	END IF;

	CALL
		common.p_set_search_path(
			format('["%1$s", "pdm_%1$s", "file_mngmt_%1$s"]', ct_tenant_id)::JSONB);


	--general comments regarding initial data loads
	-- - primary load: several attributes relating to each record (due to debugging/logging reasons) - json array (easier to handle)
	-- - ids are loaded (from json arra) into a separate pg array (in order to be able to use ANY in the next queries)
	
	--== get person details
    WITH
    	--ROW_NUMBER can't be directly in array_agg (it returns "Error [42803]: ERROR: aggregate function calls cannot contain window function calls")
    	cte_incl_rn AS (
    		SELECT
    			ROW_NUMBER() OVER () AS rn,
    			*
    		FROM
				person_vw AS pe
			WHERE TRUE
				AND (
						(cb_only_invalid AND validity = FALSE)
						OR
						NOT cb_only_invalid
					)
				--AND pe.own_external_id IN ('06156771', '7335378')
				--AND pe.external_id = 'E_sec_pos_diff_opco'
				--AND pe.party_id = '04d4e977-fb3d-4117-a0f5-e50fd2580809'
    	)
	SELECT
    	JSONB_AGG(
	        JSONB_BUILD_OBJECT(
	            'rn', rn,
	            'party_id', party_id,
	            'pe_role_id', rl_id_pers,
	            'external_id', external_id,
	            'validity', validity
	        )
		)
    INTO
    	vj_persons
    FROM
		cte_incl_rn AS c;

	--RAISE NOTICE 'vj_persons: %', JSONB_PRETTY(vj_persons);
	
	--extract person roles
	WITH
		--array_agg couldn't be combined with JSONB_ARRAY_ELEMENTS in a single statement (lateral join would be necessary)
		cte_j_ele AS (
			SELECT
				JSONB_ARRAY_ELEMENTS(vj_persons) ->> 'pe_role_id' AS id
		)
	SELECT
		ARRAY_AGG(id)
	INTO
		va_role_id_pers
	FROM
		cte_j_ele;

	--extract parties
	WITH
		--array_agg couldn't be combined with JSONB_ARRAY_ELEMENTS in a single statement (lateral join would be necessary)
		cte_j_ele AS (
			SELECT
				JSONB_ARRAY_ELEMENTS(vj_persons) ->> 'party_id' AS id
		)
	SELECT
		ARRAY_AGG(id)
	INTO
		va_party_id
	FROM
		cte_j_ele;

	RAISE NOTICE 'vj_persons.cnt: %', JSONB_ARRAY_LENGTH(vj_persons);
	RAISE NOTICE 'va_role_id_pers.cnt: %', ARRAY_LENGTH(va_role_id_pers, 1);
	RAISE NOTICE 'va_party_id.cnt: %', ARRAY_LENGTH(va_party_id, 1);
	RAISE NOTICE 'va_role_id_pers: %', va_role_id_pers;
	RAISE NOTICE 'va_party_id: %', va_party_id;
	RAISE NOTICE '';

	--== deleting
	RAISE NOTICE 'deleting party_credentials ...';
	DELETE
	FROM party_credentials AS pacr
	WHERE pacr.party_id = ANY(va_party_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting location_party ...';
	DELETE
	FROM location_party AS lp
	WHERE lp.party_id = ANY(va_party_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting party_role_group_relation ...';
	DELETE
	FROM party_role_group_relation AS rgr
	--members based
	WHERE rgr.party_role_id = ANY(va_role_id_pers);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting party_party_role_relation ...';
	DELETE
	FROM party_party_role_relation AS parl
	--members based
	WHERE parl.party_id = ANY(va_party_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting profile_user_nickname ...';
	DELETE
	FROM profile_user_nickname AS pfun
	WHERE pfun.party_id = ANY(va_party_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting profile_user ...';
	DELETE
	FROM profile_user AS pfu
	WHERE pfu.party_id = ANY(va_party_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;
	
	RAISE NOTICE 'deleting party_individual ...';
	DELETE
	FROM party_individual AS pai
	WHERE pai.id = ANY(va_party_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	--file management (because of file_def.uploaded_by_party_id)
	RAISE NOTICE 'deleting file_proc_err ...';
	DELETE FROM file_proc_err;
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting file_proc_hist ...';
	DELETE FROM file_proc_hist;
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting file_proc_stats ...';
	DELETE FROM file_proc_stats;
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting stage_gr_cust ...';
	DELETE FROM stage_gr_cust;
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting com_multi_row_errors ...';
	DELETE FROM com_multi_row_errors;
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting csv_gr_cust ...';
	DELETE FROM csv_gr_cust;
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting delta_gr_cust_com ...';
	DELETE FROM delta_gr_cust_com;
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	/*RAISE NOTICE 'deleting delta_gr_cust_fnc_rel ...';
	DELETE FROM delta_gr_cust_fnc_rel;
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;*/

	/*RAISE NOTICE 'deleting delta_gr_cust_rl_rel ...';
	DELETE FROM delta_gr_cust_rl_rel;
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;*/

	RAISE NOTICE 'deleting fnc_multi_row_errors ...';
	DELETE FROM fnc_multi_row_errors;
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting file_proc ...';
	DELETE FROM file_proc;
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting file_def ...';
	DELETE FROM file_def;
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;
	--
	
	RAISE NOTICE 'deleting party ...';
	DELETE
	FROM party AS pa
	WHERE pa.id = ANY(va_party_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting party_role ...';
	DELETE
	FROM party_role AS rl
	WHERE rl.id = ANY(va_role_id_pers);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'END: %', common.f_ts_to_char(clock_timestamp()::timestamp);
END;
$$
;