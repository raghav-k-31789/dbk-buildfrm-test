CREATE OR REFRESH MATERIALIZED VIEW `dbk-test`.`bronze`.`orders_bronze`
COMMENT 'Bronze ingestion of samples.tpch.orders for supplier analytics pipeline'
AS
WITH source_orders AS (
  SELECT
    o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderdate,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment
  FROM `samples`.`tpch`.`orders`
)
SELECT
  o_orderkey,
  o_custkey,
  o_orderstatus,
  o_totalprice,
  o_orderdate,
  o_orderpriority,
  o_clerk,
  o_shippriority,
  o_comment
FROM source_orders;