CREATE OR REFRESH MATERIALIZED VIEW `dbk-test`.`bronze`.`supplier_bronze`
AS
WITH source_supplier AS (
  -- Source-to-bronze ingestion from TPCH supplier table
  SELECT
    s_suppkey,
    s_name,
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal,
    s_comment
  FROM `samples`.`tpch`.`supplier`
)
SELECT
  s_suppkey,
  s_name,
  s_address,
  s_nationkey,
  s_phone,
  s_acctbal,
  s_comment
FROM source_supplier;