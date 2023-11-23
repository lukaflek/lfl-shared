--initial conditions
-- - LutherOne, prod-8, fg_01798570
-- - cte_head.cnt_all: 610
-- - cte_positions.cnt_all: 2584
-- - not really big data volume!!

-- version 1
--	- cte ordering: off
--  - main where clause filtering: none 
--	- runtime: 60ms
--	- comment: ctes joined using HASH JOIN
-- version 2
--	- cte ordering: off
--  - main where clause filtering: on 
--	- runtime: 7s (10x slower) <----------------- !!
--	- comment: ctes joined using NESTED LOOP (2584x)
-- version 3
--	- cte ordering: on
--  - main where clause filtering: on 
--	- runtime: 60ms (10x slower)
--	- comment: ctes joined using MERGE JOIN
EXPLAIN (ANALYZE, VERBOSE, BUFFERS, COSTS, TIMING, SUMMARY)
WITH
	cte_head AS (
		SELECT
			COUNT(*) OVER () AS cnt_all,
			rgr_m.party_role_id
		FROM
			party_role_group_relation AS rgr_m
		WHERE 1 = 1
			AND rgr_m.validity = TRUE
			AND rgr_m.entity_type = 'HeadPartyRoleGroupRelation'
			AND rgr_m.party_role_group_type = 'UnitPartyRoleGroup'
		--version 1, 2: off
		--version 3: on
		ORDER BY
  			rgr_m.party_role_id
	)
	,
	cte_positions AS (
		SELECT
			COUNT(*) OVER () AS cnt_all,
			pos.id AS party_role_id,
			p.external_id AS org_party_ext_id
		FROM
			party_role_position AS pos
			LEFT JOIN party AS p
   				ON (p.id = pos.parent_organization_id)
   		WHERE 1 = 1
     		AND p.validity = TRUE
     	--version 1, 2: off
     	--version 3: on
     	ORDER BY
      		pos.id
	)
SELECT
	COUNT(*) OVER () AS cnt_all,
	pos.org_party_ext_id,
	--==
	pos.party_role_id AS pos_party_role_id, 
	h.party_role_id AS h_party_role_id
FROM
	cte_positions AS pos 
	LEFT JOIN cte_head AS h
		ON h.party_role_id = pos.party_role_id
WHERE 1 = 1
	--version 1: off
	--version 2, 3: on
	AND pos.org_party_ext_id LIKE '%11198570%'