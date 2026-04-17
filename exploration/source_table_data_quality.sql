WITH
lineitem AS (
  SELECT * FROM samples.tpch.lineitem
),
supplier AS (
  SELECT * FROM samples.tpch.supplier
),
nation AS (
  SELECT * FROM samples.tpch.nation
),
region AS (
  SELECT * FROM samples.tpch.region
),
orders AS (
  SELECT * FROM samples.tpch.orders
),

row_counts AS (
  SELECT 'samples.tpch.lineitem' AS table_name, COUNT(*) AS row_count FROM lineitem
  UNION ALL
  SELECT 'samples.tpch.supplier' AS table_name, COUNT(*) AS row_count FROM supplier
  UNION ALL
  SELECT 'samples.tpch.nation' AS table_name, COUNT(*) AS row_count FROM nation
  UNION ALL
  SELECT 'samples.tpch.region' AS table_name, COUNT(*) AS row_count FROM region
  UNION ALL
  SELECT 'samples.tpch.orders' AS table_name, COUNT(*) AS row_count FROM orders
),

duplicate_checks AS (
  SELECT 'lineitem_duplicate_business_key' AS check_name, COUNT(*) AS issue_count
  FROM (
    SELECT l_orderkey, l_linenumber, COUNT(*) AS cnt
    FROM lineitem
    GROUP BY l_orderkey, l_linenumber
    HAVING COUNT(*) > 1
  ) d
  UNION ALL
  SELECT 'supplier_duplicate_key' AS check_name, COUNT(*) AS issue_count
  FROM (
    SELECT s_suppkey, COUNT(*) AS cnt
    FROM supplier
    GROUP BY s_suppkey
    HAVING COUNT(*) > 1
  ) d
  UNION ALL
  SELECT 'nation_duplicate_key' AS check_name, COUNT(*) AS issue_count
  FROM (
    SELECT n_nationkey, COUNT(*) AS cnt
    FROM nation
    GROUP BY n_nationkey
    HAVING COUNT(*) > 1
  ) d
  UNION ALL
  SELECT 'region_duplicate_key' AS check_name, COUNT(*) AS issue_count
  FROM (
    SELECT r_regionkey, COUNT(*) AS cnt
    FROM region
    GROUP BY r_regionkey
    HAVING COUNT(*) > 1
  ) d
  UNION ALL
  SELECT 'orders_duplicate_key' AS check_name, COUNT(*) AS issue_count
  FROM (
    SELECT o_orderkey, COUNT(*) AS cnt
    FROM orders
    GROUP BY o_orderkey
    HAVING COUNT(*) > 1
  ) d
),

null_profiling AS (
  SELECT 'lineitem.l_suppkey' AS column_name, SUM(CASE WHEN l_suppkey IS NULL THEN 1 ELSE 0 END) AS null_count FROM lineitem
  UNION ALL
  SELECT 'lineitem.l_orderkey', SUM(CASE WHEN l_orderkey IS NULL THEN 1 ELSE 0 END) FROM lineitem
  UNION ALL
  SELECT 'lineitem.l_shipdate', SUM(CASE WHEN l_shipdate IS NULL THEN 1 ELSE 0 END) FROM lineitem
  UNION ALL
  SELECT 'lineitem.l_commitdate', SUM(CASE WHEN l_commitdate IS NULL THEN 1 ELSE 0 END) FROM lineitem
  UNION ALL
  SELECT 'lineitem.l_receiptdate', SUM(CASE WHEN l_receiptdate IS NULL THEN 1 ELSE 0 END) FROM lineitem
  UNION ALL
  SELECT 'lineitem.l_quantity', SUM(CASE WHEN l_quantity IS NULL THEN 1 ELSE 0 END) FROM lineitem
  UNION ALL
  SELECT 'lineitem.l_extendedprice', SUM(CASE WHEN l_extendedprice IS NULL THEN 1 ELSE 0 END) FROM lineitem
  UNION ALL
  SELECT 'lineitem.l_discount', SUM(CASE WHEN l_discount IS NULL THEN 1 ELSE 0 END) FROM lineitem
  UNION ALL
  SELECT 'lineitem.l_tax', SUM(CASE WHEN l_tax IS NULL THEN 1 ELSE 0 END) FROM lineitem
  UNION ALL
  SELECT 'supplier.s_suppkey', SUM(CASE WHEN s_suppkey IS NULL THEN 1 ELSE 0 END) FROM supplier
  UNION ALL
  SELECT 'supplier.s_nationkey', SUM(CASE WHEN s_nationkey IS NULL THEN 1 ELSE 0 END) FROM supplier
  UNION ALL
  SELECT 'nation.n_nationkey', SUM(CASE WHEN n_nationkey IS NULL THEN 1 ELSE 0 END) FROM nation
  UNION ALL
  SELECT 'nation.n_regionkey', SUM(CASE WHEN n_regionkey IS NULL THEN 1 ELSE 0 END) FROM nation
  UNION ALL
  SELECT 'region.r_regionkey', SUM(CASE WHEN r_regionkey IS NULL THEN 1 ELSE 0 END) FROM region
  UNION ALL
  SELECT 'orders.o_orderkey', SUM(CASE WHEN o_orderkey IS NULL THEN 1 ELSE 0 END) FROM orders
),

date_validity_checks AS (
  SELECT 'lineitem_ship_before_commit' AS check_name, COUNT(*) AS issue_count
  FROM lineitem
  WHERE l_shipdate < l_commitdate
  UNION ALL
  SELECT 'lineitem_receipt_before_ship' AS check_name, COUNT(*) AS issue_count
  FROM lineitem
  WHERE l_receiptdate < l_shipdate
  UNION ALL
  SELECT 'lineitem_commit_after_receipt' AS check_name, COUNT(*) AS issue_count
  FROM lineitem
  WHERE l_commitdate > l_receiptdate
),

numeric_range_checks AS (
  SELECT 'lineitem_quantity_non_positive' AS check_name, COUNT(*) AS issue_count
  FROM lineitem
  WHERE l_quantity <= 0
  UNION ALL
  SELECT 'lineitem_extendedprice_non_positive' AS check_name, COUNT(*) AS issue_count
  FROM lineitem
  WHERE l_extendedprice <= 0
  UNION ALL
  SELECT 'lineitem_discount_out_of_range_0_1' AS check_name, COUNT(*) AS issue_count
  FROM lineitem
  WHERE l_discount < 0 OR l_discount > 1
  UNION ALL
  SELECT 'lineitem_tax_out_of_range_0_1' AS check_name, COUNT(*) AS issue_count
  FROM lineitem
  WHERE l_tax < 0 OR l_tax > 1
),

join_quality_checks AS (
  SELECT 'orphan_lineitem_to_supplier' AS check_name, COUNT(*) AS issue_count
  FROM lineitem li
  LEFT JOIN supplier s
    ON li.l_suppkey = s.s_suppkey
  WHERE s.s_suppkey IS NULL
  UNION ALL
  SELECT 'orphan_supplier_to_nation' AS check_name, COUNT(*) AS issue_count
  FROM supplier s
  LEFT JOIN nation n
    ON s.s_nationkey = n.n_nationkey
  WHERE n.n_nationkey IS NULL
  UNION ALL
  SELECT 'orphan_nation_to_region' AS check_name, COUNT(*) AS issue_count
  FROM nation n
  LEFT JOIN region r
    ON n.n_regionkey = r.r_regionkey
  WHERE r.r_regionkey IS NULL
  UNION ALL
  SELECT 'orphan_lineitem_to_orders' AS check_name, COUNT(*) AS issue_count
  FROM lineitem li
  LEFT JOIN orders o
    ON li.l_orderkey = o.o_orderkey
  WHERE o.o_orderkey IS NULL
),

shipment_timeliness_checks AS (
  SELECT
    'shipment_timeliness_distribution' AS check_name,
    SUM(CASE WHEN l_receiptdate <= l_commitdate THEN 1 ELSE 0 END) AS on_time_shipments,
    SUM(CASE WHEN l_receiptdate > l_commitdate THEN 1 ELSE 0 END) AS late_shipments,
    COUNT(*) AS total_shipments
  FROM lineitem
),

ship_delay_distribution AS (
  -- Delay definition aligned to transformation logic: datediff(receiptdate, commitdate)
  SELECT
    CASE
      WHEN DATEDIFF(l_receiptdate, l_commitdate) <= 0 THEN 'on_time_or_early'
      WHEN DATEDIFF(l_receiptdate, l_commitdate) BETWEEN 1 AND 3 THEN 'late_1_to_3_days'
      WHEN DATEDIFF(l_receiptdate, l_commitdate) BETWEEN 4 AND 7 THEN 'late_4_to_7_days'
      ELSE 'late_gt_7_days'
    END AS delay_bucket,
    COUNT(*) AS shipment_count
  FROM lineitem
  GROUP BY
    CASE
      WHEN DATEDIFF(l_receiptdate, l_commitdate) <= 0 THEN 'on_time_or_early'
      WHEN DATEDIFF(l_receiptdate, l_commitdate) BETWEEN 1 AND 3 THEN 'late_1_to_3_days'
      WHEN DATEDIFF(l_receiptdate, l_commitdate) BETWEEN 4 AND 7 THEN 'late_4_to_7_days'
      ELSE 'late_gt_7_days'
    END
),

aggregation_readiness AS (
  -- Readiness for supplier/order/ship-date grain in example.sql
  SELECT
    COUNT(*) AS total_rows_after_required_joins,
    SUM(CASE WHEN supplier_key IS NULL THEN 1 ELSE 0 END) AS null_supplier_key_rows,
    SUM(CASE WHEN order_key IS NULL THEN 1 ELSE 0 END) AS null_order_key_rows,
    SUM(CASE WHEN ship_date_key IS NULL THEN 1 ELSE 0 END) AS null_ship_date_key_rows,
    SUM(CASE WHEN nation_key IS NULL THEN 1 ELSE 0 END) AS null_nation_key_rows,
    SUM(CASE WHEN region_key IS NULL THEN 1 ELSE 0 END) AS null_region_key_rows
  FROM (
    SELECT
      li.l_suppkey AS supplier_key,
      li.l_orderkey AS order_key,
      li.l_shipdate AS ship_date_key,
      s.s_nationkey AS nation_key,
      n.n_regionkey AS region_key
    FROM lineitem li
    INNER JOIN supplier s
      ON li.l_suppkey = s.s_suppkey
    LEFT JOIN nation n
      ON s.s_nationkey = n.n_nationkey
    LEFT JOIN region r
      ON n.n_regionkey = r.r_regionkey
    LEFT JOIN orders o
      ON li.l_orderkey = o.o_orderkey
  ) x
),

final_output AS (
  SELECT 'row_count' AS section, table_name AS metric, CAST(row_count AS STRING) AS value FROM row_counts
  UNION ALL
  SELECT 'duplicate_check' AS section, check_name AS metric, CAST(issue_count AS STRING) AS value FROM duplicate_checks
  UNION ALL
  SELECT 'null_profile' AS section, column_name AS metric, CAST(null_count AS STRING) AS value FROM null_profiling
  UNION ALL
  SELECT 'date_validity' AS section, check_name AS metric, CAST(issue_count AS STRING) AS value FROM date_validity_checks
  UNION ALL
  SELECT 'numeric_range' AS section, check_name AS metric, CAST(issue_count AS STRING) AS value FROM numeric_range_checks
  UNION ALL
  SELECT 'join_quality' AS section, check_name AS metric, CAST(issue_count AS STRING) AS value FROM join_quality_checks
  UNION ALL
  SELECT 'shipment_timeliness' AS section, 'on_time_shipments' AS metric, CAST(on_time_shipments AS STRING) AS value FROM shipment_timeliness_checks
  UNION ALL
  SELECT 'shipment_timeliness' AS section, 'late_shipments' AS metric, CAST(late_shipments AS STRING) AS value FROM shipment_timeliness_checks
  UNION ALL
  SELECT 'shipment_timeliness' AS section, 'total_shipments' AS metric, CAST(total_shipments AS STRING) AS value FROM shipment_timeliness_checks
  UNION ALL
  SELECT 'ship_delay_distribution' AS section, delay_bucket AS metric, CAST(shipment_count AS STRING) AS value FROM ship_delay_distribution
  UNION ALL
  SELECT 'aggregation_readiness' AS section, 'total_rows_after_required_joins' AS metric, CAST(total_rows_after_required_joins AS STRING) AS value FROM aggregation_readiness
  UNION ALL
  SELECT 'aggregation_readiness' AS section, 'null_supplier_key_rows' AS metric, CAST(null_supplier_key_rows AS STRING) AS value FROM aggregation_readiness
  UNION ALL
  SELECT 'aggregation_readiness' AS section, 'null_order_key_rows' AS metric, CAST(null_order_key_rows AS STRING) AS value FROM aggregation_readiness
  UNION ALL
  SELECT 'aggregation_readiness' AS section, 'null_ship_date_key_rows' AS metric, CAST(null_ship_date_key_rows AS STRING) AS value FROM aggregation_readiness
  UNION ALL
  SELECT 'aggregation_readiness' AS section, 'null_nation_key_rows' AS metric, CAST(null_nation_key_rows AS STRING) AS value FROM aggregation_readiness
  UNION ALL
  SELECT 'aggregation_readiness' AS section, 'null_region_key_rows' AS metric, CAST(null_region_key_rows AS STRING) AS value FROM aggregation_readiness
)

SELECT section, metric, value
FROM final_output
ORDER BY section, metric;