CREATE OR REFRESH MATERIALIZED VIEW dbk-test.silver.silver_supplier_analytics
AS
WITH lineitem_src AS (
  SELECT
    CAST(l_orderkey AS BIGINT) AS order_key,
    CAST(l_suppkey AS BIGINT) AS supplier_key,
    CAST(l_shipdate AS DATE) AS ship_date,
    CAST(l_commitdate AS DATE) AS commit_date,
    CAST(l_receiptdate AS DATE) AS receipt_date,
    CAST(l_quantity AS DECIMAL(18,2)) AS quantity,
    CAST(l_extendedprice AS DECIMAL(18,2)) AS extended_price,
    CAST(l_discount AS DECIMAL(10,6)) AS discount_rate,
    CAST(l_tax AS DECIMAL(10,6)) AS tax_rate,
    l_returnflag AS return_flag,
    l_linestatus AS line_status,
    l_shipinstruct AS ship_instruct,
    l_shipmode AS ship_mode
  FROM dbk-test.bronze.lineitem_bronze
),
orders_src AS (
  SELECT
    CAST(o_orderkey AS BIGINT) AS order_key,
    CAST(o_custkey AS BIGINT) AS customer_key,
    o_orderstatus AS order_status,
    CAST(o_totalprice AS DECIMAL(18,2)) AS order_total_price,
    CAST(o_orderdate AS DATE) AS order_date,
    o_orderpriority AS order_priority,
    o_clerk AS clerk,
    CAST(o_shippriority AS INT) AS ship_priority
  FROM dbk-test.bronze.orders_bronze
),
supplier_src AS (
  SELECT
    CAST(s_suppkey AS BIGINT) AS supplier_key,
    s_name AS supplier_name,
    CAST(s_nationkey AS BIGINT) AS nation_key,
    s_phone AS supplier_phone,
    CAST(s_acctbal AS DECIMAL(18,2)) AS supplier_account_balance
  FROM dbk-test.bronze.supplier_bronze
),
nation_src AS (
  SELECT
    CAST(n_nationkey AS BIGINT) AS nation_key,
    n_name AS nation_name,
    CAST(n_regionkey AS BIGINT) AS region_key
  FROM dbk-test.bronze.nation_bronze
),
region_src AS (
  SELECT
    CAST(r_regionkey AS BIGINT) AS region_key,
    r_name AS region_name
  FROM dbk-test.bronze.region_bronze
),
silver_enriched AS (
  SELECT
    li.supplier_key,
    li.order_key,
    li.ship_date,
    sp.nation_key,
    nt.region_key,
    sp.supplier_name,
    nt.nation_name,
    rg.region_name,
    od.customer_key,
    od.order_status,
    od.order_total_price,
    od.order_date,
    od.order_priority,
    od.clerk,
    od.ship_priority,
    li.quantity,
    li.extended_price AS gross_revenue,
    li.extended_price * (1 - li.discount_rate) AS net_revenue,
    li.discount_rate AS discount,
    li.tax_rate AS tax,
    li.return_flag,
    li.line_status,
    li.ship_instruct,
    li.ship_mode,
    li.commit_date,
    li.receipt_date,
    -- Shipment reliability flags derived from receipt vs commit dates
    CASE WHEN li.receipt_date <= li.commit_date THEN 1 ELSE 0 END AS on_time_flag,
    CASE WHEN li.receipt_date > li.commit_date THEN 1 ELSE 0 END AS late_flag,
    -- Delay in days; non-late shipments are set to 0 for business-friendly interpretation
    GREATEST(DATEDIFF(li.receipt_date, li.commit_date), 0) AS ship_delay_days
  FROM lineitem_src li
  INNER JOIN orders_src od
    ON li.order_key = od.order_key
  INNER JOIN supplier_src sp
    ON li.supplier_key = sp.supplier_key
  INNER JOIN nation_src nt
    ON sp.nation_key = nt.nation_key
  INNER JOIN region_src rg
    ON nt.region_key = rg.region_key
)
SELECT
  supplier_key,
  order_key,
  ship_date,
  nation_key,
  region_key,
  supplier_name,
  nation_name,
  region_name,
  customer_key,
  order_status,
  order_total_price,
  order_date,
  order_priority,
  clerk,
  ship_priority,
  quantity,
  gross_revenue,
  net_revenue,
  discount,
  tax,
  return_flag,
  line_status,
  ship_instruct,
  ship_mode,
  commit_date,
  receipt_date,
  on_time_flag,
  late_flag,
  ship_delay_days
FROM silver_enriched;