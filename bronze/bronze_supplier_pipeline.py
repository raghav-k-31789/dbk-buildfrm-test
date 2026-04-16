# Databricks notebook source
# COMMAND ----------
import dlt
from pyspark.sql import functions as F

SOURCE_CATALOG = "samples"
SOURCE_SCHEMA = "tpch"
TARGET_CATALOG = "workspace"
TARGET_SCHEMA = "bronze"

# COMMAND ----------
@dlt.table(
    name="lineitem_bronze",
    comment="Bronze materialized view of TPCH lineitem with light standardization and ingest metadata."
)
def lineitem_bronze():
    return (
        spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.lineitem")
        .select(
            F.col("l_orderkey").cast("long").alias("l_orderkey"),
            F.col("l_partkey").cast("long").alias("l_partkey"),
            F.col("l_suppkey").cast("long").alias("l_suppkey"),
            F.col("l_linenumber").cast("int").alias("l_linenumber"),
            F.col("l_quantity").cast("double").alias("l_quantity"),
            F.col("l_extendedprice").cast("double").alias("l_extendedprice"),
            F.col("l_discount").cast("double").alias("l_discount"),
            F.col("l_tax").cast("double").alias("l_tax"),
            F.col("l_returnflag").cast("string").alias("l_returnflag"),
            F.col("l_linestatus").cast("string").alias("l_linestatus"),
            F.to_date("l_shipdate").alias("l_shipdate"),
            F.to_date("l_commitdate").alias("l_commitdate"),
            F.to_date("l_receiptdate").alias("l_receiptdate"),
            F.trim(F.col("l_shipinstruct")).alias("l_shipinstruct"),
            F.trim(F.col("l_shipmode")).alias("l_shipmode"),
            F.trim(F.col("l_comment")).alias("l_comment"),
            F.current_timestamp().alias("_bronze_ingest_ts"),
            F.lit(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.lineitem").alias("_bronze_source")
        )
    )

# COMMAND ----------
@dlt.table(
    name="orders_bronze",
    comment="Bronze materialized view of TPCH orders with light standardization and ingest metadata."
)
def orders_bronze():
    return (
        spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.orders")
        .select(
            F.col("o_orderkey").cast("long").alias("o_orderkey"),
            F.col("o_custkey").cast("long").alias("o_custkey"),
            F.trim(F.col("o_orderstatus")).alias("o_orderstatus"),
            F.col("o_totalprice").cast("double").alias("o_totalprice"),
            F.to_date("o_orderdate").alias("o_orderdate"),
            F.trim(F.col("o_orderpriority")).alias("o_orderpriority"),
            F.trim(F.col("o_clerk")).alias("o_clerk"),
            F.col("o_shippriority").cast("int").alias("o_shippriority"),
            F.trim(F.col("o_comment")).alias("o_comment"),
            F.current_timestamp().alias("_bronze_ingest_ts"),
            F.lit(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.orders").alias("_bronze_source")
        )
    )

# COMMAND ----------
@dlt.table(
    name="supplier_bronze",
    comment="Bronze materialized view of TPCH supplier with light standardization and ingest metadata."
)
def supplier_bronze():
    return (
        spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.supplier")
        .select(
            F.col("s_suppkey").cast("long").alias("s_suppkey"),
            F.trim(F.col("s_name")).alias("s_name"),
            F.trim(F.col("s_address")).alias("s_address"),
            F.col("s_nationkey").cast("long").alias("s_nationkey"),
            F.trim(F.col("s_phone")).alias("s_phone"),
            F.col("s_acctbal").cast("double").alias("s_acctbal"),
            F.trim(F.col("s_comment")).alias("s_comment"),
            F.current_timestamp().alias("_bronze_ingest_ts"),
            F.lit(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.supplier").alias("_bronze_source")
        )
    )

# COMMAND ----------
@dlt.table(
    name="nation_bronze",
    comment="Bronze materialized view of TPCH nation with light standardization and ingest metadata."
)
def nation_bronze():
    return (
        spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.nation")
        .select(
            F.col("n_nationkey").cast("long").alias("n_nationkey"),
            F.trim(F.col("n_name")).alias("n_name"),
            F.col("n_regionkey").cast("long").alias("n_regionkey"),
            F.trim(F.col("n_comment")).alias("n_comment"),
            F.current_timestamp().alias("_bronze_ingest_ts"),
            F.lit(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.nation").alias("_bronze_source")
        )
    )

# COMMAND ----------
@dlt.table(
    name="region_bronze",
    comment="Bronze materialized view of TPCH region with light standardization and ingest metadata."
)
def region_bronze():
    return (
        spark.table(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.region")
        .select(
            F.col("r_regionkey").cast("long").alias("r_regionkey"),
            F.trim(F.col("r_name")).alias("r_name"),
            F.trim(F.col("r_comment")).alias("r_comment"),
            F.current_timestamp().alias("_bronze_ingest_ts"),
            F.lit(f"{SOURCE_CATALOG}.{SOURCE_SCHEMA}.region").alias("_bronze_source")
        )
    )

# COMMAND ----------
display(spark.sql(f"SELECT * FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.lineitem_bronze LIMIT 20"))