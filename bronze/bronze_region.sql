CREATE OR REFRESH MATERIALIZED VIEW `dbk-test`.`bronze`.`region_bronze`
AS
WITH source_region AS (
  -- Source-to-bronze ingestion from TPC-H region reference data
  SELECT
    r_regionkey,
    r_name,
    r_comment
  FROM `samples`.`tpch`.`region`
)
SELECT
  r_regionkey,
  r_name,
  r_comment
FROM source_region;