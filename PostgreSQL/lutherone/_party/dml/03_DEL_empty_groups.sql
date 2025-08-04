--------------------------------------------------
--group is considered empty if it has no participants (not even the head). that's because
-- - we can't afford deleting OpCo group (head: OpCo rl) once is hasn't any members because the "load positions" process assumes the group exists
-- - units/departments are completely memberless if all positions were deleted before this script 
--------------------------------------------------

DO
$$
DECLARE
	ct_tenant_id constant	TEXT := 'k_2019418'; --lpc_0000001, k_2019418
	vi_rows_affected		INT;
	--
	vt_hostname		TEXT;
	--
	vi_cnt_group	INT;
	vj_group		JSONB;
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

    WITH
    	--ROW_NUMBER can't be directly in array_agg (it returns "Error [42803]: ERROR: aggregate function calls cannot contain window function calls")
    	cte_incl_rn AS (
			SELECT
    			ROW_NUMBER() OVER () 	AS rn
				,gr.id
				,MAX(gr.entity_type)	AS entity_type
				,MAX(gr.group_name)		AS group_name
				,COUNT(rlgr.*)			AS pcpt_cnt
			FROM
				cde_party_role_group_vw				AS gr
				LEFT JOIN party_role_group_relation	AS rlgr
					ON rlgr.party_role_group_id = gr.id
				/*LEFT JOIN party_role				AS rl
					ON rl.id = rlgr.party_role_id
					AND rl.validity*/
			WHERE TRUE
   				--AND gr.entity_type = ANY(common.c_auto_managed_party_role_group_types())
			GROUP BY
			    gr.id
			HAVING
				COUNT(rlgr.*) = 0
    	)
	SELECT
    	JSONB_AGG(
	        JSONB_BUILD_OBJECT(
	            'rn', rn,
	            'id', id,
	            'entity_type', entity_type,
	            'name', group_name,
	            'pcpt_cnt', pcpt_cnt
	        )
		)
    INTO
    	vj_group
    FROM
		cte_incl_rn AS c;

	--RAISE NOTICE 'vj_group: %', JSONB_PRETTY(vj_group);
	
	vi_cnt_group := JSONB_ARRAY_LENGTH(vj_group);

	WITH
		--array_agg couldn't be combined with JSONB_ARRAY_ELEMENTS in a single statement (lateral join would be necessary)
		cte_j_ele AS (
			SELECT
				JSONB_ARRAY_ELEMENTS(vj_group) ->> 'id' AS id
		)
	SELECT
		array_agg(id)
	INTO
		va_group_id
	FROM
		cte_j_ele;

	RAISE NOTICE 'vi_cnt_group: %', vi_cnt_group;
	--RAISE NOTICE 'vj_position: %', JSONB_PRETTY(vj_position);
	RAISE NOTICE 'va_group_id: %', va_group_id;
	RAISE NOTICE '';

	--== deleting
	RAISE NOTICE 'deleting party_role_group_association ...';
	DELETE
	FROM party_role_group_association AS grass
	WHERE grass.party_role_group_involved_id = ANY(va_group_id) OR grass.party_role_group_id = ANY(va_group_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE 'deleting party_role_group ...';
	DELETE
	FROM party_role_group AS g
	WHERE g.id = ANY(va_group_id);
	GET DIAGNOSTICS vi_rows_affected = ROW_COUNT;
	RAISE NOTICE 'deleted: %', vi_rows_affected;

	RAISE NOTICE '';
	RAISE NOTICE 'END: %', common.f_ts_to_char(clock_timestamp()::timestamp);
END;
$$
;
