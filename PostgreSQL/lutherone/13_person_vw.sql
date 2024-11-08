SELECT tenant_id, parent_company_name, validity FROM public.tenant;
--SET search_path TO k_2019418, pdm_k_2019418;
/*
CALL
	common.p_set_search_path(
		format('["%1$s", "pdm_%1$s", "public", "lfl_test"]', 'k_2019418')::JSONB
	);
*/

--== materialize test cde_party_individual_vw
-- using the view significantly slows down some other views - especially position_person_rel_vw ("select *" decreases from 650ms to 42) and afterwards group_role_rel_vw_ext_2 as well (even greater
-- performance impact). using materialized data seems to be the proper remedy 
DROP TABLE IF EXISTS lfl_test.party_individual_decrypt CASCADE;

--cde_party_individual_vw: 14s, 11k rows
--party_individual: 96ms, 11k rows
CREATE TABLE lfl_test.party_individual_decrypt
AS
SELECT
	*
	,clock_timestamp() AS ins_dt
FROM
	--party_individual
	cde_party_individual_vw
WHERE TRUE
	AND id = 'f34e9680-ab48-442f-9569-0973579d57ee'
;

--SELECT * FROM lfl_test.party_individual_decrypt WHERE id = 'f34e9680-ab48-442f-9569-0973579d57ee';
--== /materialize

DROP VIEW IF EXISTS lfl_test.person_vw CASCADE;

CREATE OR REPLACE VIEW lfl_test.person_vw
AS
WITH
	cte_loc AS (
		SELECT
			lp.party_id
			,ARRAY_AGG(lp.id) AS id_arr
			,ARRAY_AGG(
				format(
					'%s (%s) (%s; %s)'
					,lp.entity_type
					,COALESCE(lp.phone_number, lp.email_address, format('%s, %s, %s', lp.city, lp.country, lp.region), lp.device_token)
					,lp.id::TEXT
					,lp.validity::TEXT
				)
			) AS arr_verbose
		FROM
		    cde_location_party_vw AS lp
		WHERE TRUE
		    --AND lp.validity
		GROUP BY
			lp.party_id
	)
	,cte_cred AS (
		SELECT
			cr.party_id
			,max(CASE WHEN cr.validity THEN cr.id::TEXT ELSE NULL END)::UUID AS id
			,max(CASE WHEN cr.validity THEN cr.username ELSE NULL END) AS username
			,ARRAY_AGG(
				format(
					'%s (%s) (%s; %s)'
					,cr.entity_type
					,cr.username
					,cr.id::TEXT
					,cr.validity::TEXT
				)
			) AS arr_verbose
		FROM
		    party_credentials AS cr
		WHERE TRUE
			--AND cr.validity
			--AND cr.party_id = 'ba485766-5768-4d11-b1c1-3b4000ccbf29'::UUID
		GROUP BY
			cr.party_id
	)
	,cte_pos AS (
		SELECT
			parl.party_id
			,ARRAY_AGG(parl.role_id) AS arr_id
			,ARRAY_AGG(
				format('%s (%s)', parl.rl_external_id, parl.role_id)
			) AS arr_verbose
		FROM
			lfl_test.party_role_rel_vw_ext AS parl
		WHERE TRUE
			AND parl.pa_validity
			AND parl.rl_validity
	 		AND parl.rl_entity_type = 'position'
	 		--test
	 		--AND parl.pa_external_id = 'E_ORE3'
		GROUP BY
			parl.party_id
	)
	--returns the newest record, valid are preferred over invalid (so if it happens a valid record is not the newest, the valid is the pick)
	,cte_pf_nick AS (
		SELECT DISTINCT ON (party_id)
			*
		FROM
			profile_user_nickname AS pu
		WHERE 1 = 1
			--AND pu.party_id IN ('21a237fe-f8bf-475a-bcb5-a5746e993b43', 'd8df1f22-4528-404f-a6ab-40a401b61b26', '30264fc4-bc40-477a-ad10-c96af743aef4')
		ORDER BY
			party_id,
			--
			CASE WHEN pu.validity = FALSE THEN 1 ELSE 2 END DESC,
			valid_from DESC
	)
	--returns the newest record, valid are preferred over invalid (so if it happens a valid record is not the newest, the valid is the pick)
	--!! DRY BREACH: the same cte is in company_vw !! 
	,cte_pf AS (
		SELECT DISTINCT ON (party_id)
			*,
			--debug
			to_timestamp(pu.tracking_info ->> 'modified', 'YYYY-MM-DDThh24:mi:ss.msZ') AS modified_dt
		FROM
			profile_user AS pu
		ORDER BY
			party_id,
			--
			CASE WHEN pu.validity = FALSE THEN 1 ELSE 2 END DESC,
			to_timestamp(pu.tracking_info ->> 'modified', 'YYYY-MM-DDThh24:mi:ss.msZ') DESC
	)
SELECT
	h.id 			AS party_id,
	h.entity_type,
	h.entity_type_orig,
	h.validity,
	h.external_id,
	h.tr_created	AS pa_tr_created,
	h.tr_modified	AS pa_tr_modified,
	--
	pi.first_name,
	pi.last_name,
	CASE
		WHEN pi.first_name IS NOT NULL AND pi.last_name IS NOT NULL
		THEN format('%s %s', pi.first_name, pi.last_name)
	END 									AS name_full,
	pi.active 								AS is_active,
	pi.party_in_use 						AS is_in_use,
	pfu.parent_organization_id 				AS own_party_id,
	co.external_id 							AS own_external_id,
	co.name 								AS own_name,
	pfu.parent_organization_party_role_id	AS own_role_id,
	rlo.entity_type	AS own_rl_entity_type,
	pape.role_id 	AS pe_role_id,
	po.arr_verbose 	AS po_arr_verbose,
	pfu.job_title 	AS pfu_job_title,
	pfu.nickname 	AS pfu_nickname,
	cr.username 	AS cr_username, 
	--ids to other party related tables 
	pi.id			AS id_pai,
	pfu.id 			AS id_pfu,
	pfn.id 			AS id_pf_nick,
	cr.id 			AS id_cred,
	loc.id_arr 		AS id_loc_arr,
	loc.arr_verbose AS loc_arr_verbose,
	cr.arr_verbose AS cr_arr_verbose
FROM
	lfl_test.party_vw AS h --HEADER
	--details
	--LEFT JOIN cde_party_individual_vw AS pi
	LEFT JOIN lfl_test.party_individual_decrypt AS pi
  		ON pi.id = h.id
  	LEFT JOIN cte_pf AS pfu
   		ON pfu.party_id = h.id
    LEFT JOIN cte_pf_nick AS pfn
    	ON pfn.party_id = h.id
    LEFT JOIN cte_loc AS loc
    	ON loc.party_id = h.id
    LEFT JOIN cte_cred AS cr
    	ON cr.party_id = h.id
   	--owner
   	LEFT JOIN lfl_test.company_vw AS co
    	ON co.party_id = pfu.parent_organization_id
    --cannot be used because role_vw_ext_own references person_vw - so circular reference would get created
    /*LEFT JOIN lfl_test.role_vw_ext_own AS rlo --role owner
  		ON (rlo.id = pfu.parent_organization_party_role_id)*/
    LEFT JOIN lfl_test.role_vw AS rlo
    	ON rlo.id = pfu.parent_organization_party_role_id
    --
    LEFT JOIN lfl_test.party_role_rel_vw_ext AS pape
    	ON pape.party_id = h.id
    	AND pape.rl_entity_type = 'person'
    LEFT JOIN cte_pos AS po
    	ON po.party_id = h.id
WHERE 1 = 1
	AND h.entity_type = 'individual'
	--party_id/validity=true is currently not enforced to be unique, but should be (https://lutherx.atlassian.net/browse/LUT-6463)
	--deliberatelly commented because the respective CTEs pick the most relevant record. use-cases to consider if the conditions are applied
	-- - there might be a single record, that is invalid - the following conditions would cause data loss
	-- - a record presence is not enforced
	--AND (pfu.validity = TRUE OR pfu.validity IS NULL) --avoids row multiplication (0..n rows might be in profile_user)
	--AND (pfn.validity = TRUE OR pfn.validity IS NULL) --avoids row multiplication (0..n rows might be in profile_user_nickname)
	--test
	--AND h.external_id = 'E_ORE3'
	--AND h.id = 'f34e9680-ab48-442f-9569-0973579d57ee'
;

/*SELECT
	*
FROM
	lfl_test.person_vw AS pe
WHERE 1 = 1
	AND pe.validity
	--AND pe.party_id IN ('f34e9680-ab48-442f-9569-0973579d57ee')
	--AND pe.role_id_pers = '32933eba-907b-403b-8452-bba8c390d082'
	--AND p.own_external_id = '06156771'
	--AND p.own_external_id IS NULL
	--AND lower(pe.external_id) LIKE lower('%TST_EMPID%')
	AND pe.external_id IN ('E_ORE1')
	--AND cr_username = 'emilia.chacinska@bnpparibas.pl'
	--AND pe.pfu_nickname = 'maya.nikiforova'
	/*AND pe.pfu_nickname  ~ 
		concat(
			lower(
				concat(
					replace(public.unaccent(pe.first_name), ' ', ''),
					'.',
					replace(public.unaccent(pe.last_name), ' ', '')
				)
			)
			, '\.\d+$'
		)*/
ORDER BY
	own_external_id
;*/

--overview
/*SELECT
	pe.validity
	,pe.own_external_id
	,pe.own_rl_entity_type
	,MAX(pe.own_name)		AS own_name
	--TO_CHAR(pa_tr_modified, 'yyyy-mm-dd') AS pa_tr_modified
	--TO_CHAR(pa_tr_created, 'yyyy-mm-dd') AS pa_tr_created
    --
	,COUNT(*)											AS cnt
	,SUM(COUNT(*)) OVER ()								AS cnt_all
	,ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 1)	AS cnt_pct
FROM
	lfl_test.person_vw AS pe
WHERE 1 = 1
	AND pe.validity
GROUP BY
	pe.validity
	,pe.own_external_id
	,pe.own_rl_entity_type
	--TO_CHAR(pa_tr_modified, 'yyyy-mm-dd')
	--TO_CHAR(pa_tr_created, 'yyyy-mm-dd')
ORDER BY
	cnt DESC
	--pa_tr_modified DESC
	--pa_tr_created DESC
;*/

--the same external_id in more companies
/*SELECT
	pe.external_id
    --
	,COUNT(*) AS cnt
	,SUM(COUNT(*)) OVER () AS cnt_all
	,ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 1) AS cnt_pct
FROM
	lfl_test.person_vw AS pe
WHERE 1 = 1
	AND pe.validity
GROUP BY
	pe.external_id
HAVING
	COUNT(*) > 1
ORDER BY
	cnt DESC
;*/

--checks
/*SELECT
	p.party_id,
	COUNT(*) as cnt
FROM
	lfl_test.person_vw AS p
WHERE 1 = 1
	AND p.first_name IS NULL
GROUP BY
    p.party_id
HAVING
	COUNT(*) > 1
;*/