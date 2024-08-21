--================
-- number of items in an json array ("length")
--================
WITH
	cte_json AS (
		SELECT
			'{
			    "header_id": "header_val_1",
			    "items_A": [
			        {
			            "item_id": 1,
						"item_val": "val_1"
			        },
			        {
			            "item_id": 2,
						"item_val": "val_2"
			        },
			        {
			            "item_id": 3,
						"item_val": "val_3"
			        },
			        {
			            "item_id": 4,
						"item_val": "val_2"
			        },
			        {
			            "item_id": 5,
						"item_val": "val_4"
			        }
		    	],
			    "items_B": [
			        {
			            "item_id": 1,
						"item_val": "val_1"
			        },
			        {
			            "item_id": 2,
						"item_val": "val_2"
			        }
		    	],
			    "items_C": [],
				"items_D": null
			}'::JSONB AS val_json
	)
SELECT
	val_json
	--
	,val_json -> 'items_A' AS items_A
	,JSONB_ARRAY_LENGTH(val_json -> 'items_A') AS cnt_items_A
	--
	,val_json -> 'items_B' AS items_B
	,JSONB_ARRAY_LENGTH(val_json -> 'items_B') AS cnt_items_B
	--
	,val_json -> 'items_C' AS items_C
	,JSONB_ARRAY_LENGTH(val_json -> 'items_C') AS cnt_items_C
	--
	,val_json -> 'items_D' AS items_D_js
	,val_json ->> 'items_D' AS items_D_tx
	--ERROR: cannot get array length of a scalar
	--JSONB_ARRAY_LENGTH(val_json -> 'items_D') AS cnt_items_D
	--option 1
	,CASE
		WHEN (val_json -> 'items_D') = 'null'
		THEN 0
		ELSE JSONB_ARRAY_LENGTH(val_json -> 'items_D')
	END AS cnt_items_D_1
	--option 2
	,CASE
		WHEN (val_json ->> 'items_D') IS NULL
		THEN 0
		ELSE JSONB_ARRAY_LENGTH(val_json -> 'items_D')
	END AS cnt_items_D_2
	--option 3
	,JSONB_ARRAY_LENGTH(
		CASE
			WHEN jsonb_typeof(val_json -> 'items_D') = 'null' THEN '[]'::jsonb
			ELSE val_json -> 'items_D'
		END
	) AS cnt_items_D_3
	--option 4
	/*,JSONB_ARRAY_LENGTH(
		COALESCE((val_json ->> 'items_D')::JSONB, '[]'::JSONB)
	) AS cnt_items_D_4*/
	,JSONB_ARRAY_LENGTH(
		COALESCE((val_json ->> 'items_D'), '[]')::JSONB
	) AS cnt_items_D_5
FROM
	cte_json;