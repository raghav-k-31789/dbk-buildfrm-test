CREATE OR REFRESH MATERIALIZED VIEW dbk-test.bronze.nation_bronze
AS
WITH source_nation AS (
  -- Bronze ingestion: land raw nation records from TPCH sample source with no business transformation
  SELECT
    n_nationkey,
    n_name,
    n_regionkey,
    n_comment
  FROM samples.tpch.nation
)
SELECT
  n_nationkey,
  n_name,
  n_regionkey,
  n_comment
FROM source_nation;