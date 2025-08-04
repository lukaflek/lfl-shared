DO
$$
DECLARE
	ct_tenant_id constant	TEXT := 'k_2019418';
	cb_only_invalid			BOOLEAN := FALSE;
	vi_rows_affected		INT;
	--
	vt_hostname		TEXT;
	--
	vi_cnt_pos		INT;
	vi_cnt_group	INT;
	--
	vj_position		JSONB;
	vj_group		JSONB;
	--
	va_pos_id		UUID[];
	va_group_id		UUID[];
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
			format('["%1$s", "pdm_%1$s"]', ct_tenant_id)::JSONB);

	--general comments regarding initial data loads
	-- - primary load: several attributes relating to each record (due to debugging/logging reasons) - json array (easier to handle)
	-- - ids are loaded (from json arra) into a separate pg array (in order to be able to use ANY in the next queries)
	
	--== get positions
    WITH
    	--ROW_NUMBER can't be directly in array_agg (it returns "Error [42803]: ERROR: aggregate function calls cannot contain window function calls")
    	cte_incl_rn AS (
    		SELECT
    			ROW_NUMBER() OVER () AS rn,
    			*
    		FROM
				position_vw AS po
			WHERE 1 = 1
				AND (
						(cb_only_invalid AND validity = FALSE)
						OR
						NOT cb_only_invalid
					)
				--AND po.validity
				--AND po.own_external_id = '7335378'
				--AND po.external_id = 'ORE99'
			
			--unoccupied only
			/*SELECT
				ROW_NUMBER() OVER () AS rn
				,po_role_id 	role_id
				,po_external_id	external_id
				,po_name		"name"
			FROM
				lfl_test.position_person_rel_vw AS pope
			WHERE 1 = 1
				--AND pope.own_external_id = '7335377'
				--AND pope.pe_party_id IS NULL
				AND pope.po_external_id = 'TST_EXTID'*/
    	)
	SELECT
    	JSONB_AGG(
	        JSONB_BUILD_OBJECT(
	            'rn', rn,
	            'id', id,
	            'external_id', external_id,
	            'name', name,
				'validity', validity
	        )
		)
    INTO
    	vj_position
    FROM
		cte_incl_rn AS c;

	RAISE NOTICE 'vj_position: %', JSONB_PRETTY(vj_position);
	
	vi_cnt_pos := JSONB_ARRAY_LENGTH(vj_position);

	WITH
		--array_agg couldn't be combined with JSONB_ARRAY_ELEMENTS in a single statement (lateral join would be necessary)
		cte_j_ele AS (
			SELECT
				JSONB_ARRAY_ELEMENTS(vj_position) ->> 'id' AS id
		)
	SELECT
		array_agg(id)
	INTO
		va_pos_id
	FROM
		cte_j_ele;

	RAISE NOTICE 'vi_cnt_pos: %', vi_cnt_pos;
	--RAISE NOTICE 'vj_position: %', JSONB_PRETTY(vj_position);
	RAISE NOTICE 'va_pos_id: %', va_pos_id;
	RAISE NOTICE '';

	--== get groups (that are headed by any deleted position - these might be deleted completely)
	WITH
    	cte_incl_rn AS (
    		SELECT
    			'rn', ROW_NUMBER() OVER () AS rn,
    			*
			FROM
				group_vw AS g
			WHERE 1 = 1
				AND g.head_po_id = ANY(va_pos_id)
				--other ones will be kept (definition), just members removed 
				AND g.entity_type IN ('unit', 'department')
    	)
	SELECT
    	JSONB_AGG(
	        JSONB_BUILD_OBJECT(
	            'rn', rn,
	            'id', id,
	            'type', entity_type,
	            'name', name
	        )
		)
    INTO
    	vj_group
    FROM
		cte_incl_rn AS c;
	
	vi_cnt_group := JSONB_ARRAY_LENGTH(vj_group);

	WITH
		--array_agg couldn't be combined with JSONB_ARRAY_ELEMENTS in a single statement (lateral join would be necessary)
		cte_j_ele AS (
			SELECT
				JSONB_ARRAY_ELEMENTS(vj_group) ->> 'id' AS id
		)
	SELECT
		ARRAY_AGG(id)
	INTO
		va_group_id
	FROM
		cte_j_ele;

	RAISE NOTICE 'vi_cnt_group: %', vi_cnt_group;
	--RAISE NOTICE 'vj_group: %', vj_group;
	RAISE NOTICE 'va_group_id: %', va_group_id;
	RAISE NOTICE '';

	--== deleting
	RAISE NOTICE 'deleting party_role_group_relation ...';
	DELETE
	FROM party_role_group_relation AS rgr
	--members based
	WHERE rgr.party_role_id = ANY(va_pos_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting party_role_group_association ...';
	DELETE
	FROM party_role_group_association AS gras
	WHERE gras.party_role_group_id = ANY(va_group_id)
		OR gras.party_role_group_involved_id = ANY(va_group_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting party_role_group ...';
	DELETE
	FROM party_role_group AS g
	WHERE g.id = ANY(va_group_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting party_role_association ...';
	DELETE
	FROM party_role_association AS rlas
	WHERE rlas.party_role_id = ANY(va_pos_id) OR rlas.party_role_involved_id = ANY(va_pos_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting party_role_position ...';
	DELETE
	FROM party_role_position AS po
	WHERE po.id = ANY(va_pos_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting party_role ...';
	DELETE
	FROM party_role AS rl
	WHERE rl.id = ANY(va_pos_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE '';
	RAISE NOTICE 'END: %', common.f_ts_to_char(clock_timestamp()::timestamp);
END;
$$
;