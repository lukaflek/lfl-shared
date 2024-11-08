SELECT tenant_id, parent_company_name, validity FROM public.tenant;
--SET search_path TO k_2019418, pdm_k_2019418;
/*
CALL
	common.p_set_search_path(
		format(
			'["%1$s", "pdm_%1$s", "public", "lfl_test"]',
			'k_2019418'
		)::JSONB
	);
*/

DROP VIEW IF EXISTS lfl_test.position_vw CASCADE;

CREATE OR REPLACE VIEW lfl_test.position_vw
AS
SELECT
	h.id AS role_id,
	h.entity_type,
	h.entity_type_orig,
	h.validity,
	h.external_id, --it's the position role external_id
	d.position_name AS name,
	d.hierarchy_level = 1 AS is_ceo,
	/*f_is_ceo_main(
		iu_co_owner_pa_id 	=> d.parent_organization_id,
		ii_po_hierarchy_lvl => d.hierarchy_level
	) AS is_ceo_main,*/
	d.parent_organization_id AS own_party_id,
	co.external_id AS own_external_id,
	co.name AS own_name,
	d.department_name AS dep_name,
	d.collar_type,
	d.job_family ,
	d.position_type,
	d.hierarchy_level,
	h.tr_created,
	h.tr_modified,
	h.tr_version
FROM
 	lfl_test.role_vw AS h --header
  	LEFT JOIN cde_party_role_position_vw AS d --detail
   		ON (d.id = h.id)
   	LEFT JOIN lfl_test.company_vw AS co
    	ON (co.party_id = d.parent_organization_id)
WHERE 1 = 1
	AND h.entity_type = 'position'
;

/*SELECT
	*
FROM
	lfl_test.position_vw AS po
WHERE 1 = 1
	--AND po.validity
	--AND po.role_id IN ('46e6ee10-5098-4809-b76b-e42d2121d3f0')
	--AND po.hierarchy_level = 1
	--AND po.is_ceo = TRUE
	AND po.external_id IN ('PR2')
	--AND lower(po.external_id) LIKE lower('TST_POSID%')
	--AND po.own_external_id = '17151088'
	--AND po.own_external_id IS NULL
	--AND po.name = 'Devops'
ORDER BY
	po.own_external_id,
	po.hierarchy_level,
	po.external_id
;*/

--overview
SELECT
	ps.validity
	,ps.own_external_id
	,ps.own_name
	,ps.entity_type
    --
	,COUNT(*) AS cnt
	,SUM(COUNT(*)) OVER () AS cnt_all
	,ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 1) AS cnt_pct
FROM
	lfl_test.position_vw AS ps
WHERE 1 = 1
	AND ps.validity
GROUP BY
	ps.validity
	,ps.own_external_id
	,ps.own_name
	,ps.entity_type
ORDER BY
	cnt DESC
;

--the same external_id in more companies
/*SELECT
	ps.external_id
    --
	,COUNT(*) AS cnt
	,SUM(COUNT(*)) OVER () AS cnt_all
	,ROUND(COUNT(*) / SUM(COUNT(*)) OVER () * 100, 1) AS cnt_pct
FROM
	lfl_test.position_vw AS ps
WHERE 1 = 1
	AND ps.validity
GROUP BY
	ps.external_id
HAVING
	COUNT(*) > 1
ORDER BY
	cnt DESC
;*/


--checks
/*SELECT *
FROM
	lfl_test.position_vw AS po
WHERE 1 = 1
	AND po.name IS NULL
;*/