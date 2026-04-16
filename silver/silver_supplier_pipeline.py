# Databricks notebook source
# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
@dlt.table(
    name="lineitem_silver",
    comment="Cleaned lineitem records with shipment and revenue derivations."
)
@dlt.expect("valid_orderkey", "order_key IS NOT NULL")
@dlt.expect("valid_suppkey", "supplier_key IS NOT NULL")
@dlt.expect("valid_shipdate", "ship_date IS NOT NULL")
@dlt.expect(
    "valid_numeric_ranges",
    "quantity >= 0 AND extended_price >= 0 AND discount >= 0 AND discount <= 1 AND tax >= 0"
)
def lineitem_silver():
    df = spark.read.table("workspace.bronze.lineitem_bronze").select(
        F.col("l_orderkey").alias("order_key"),
        F.col("l_suppkey").alias("supplier_key"),
        F.col("l_shipdate").alias("ship_date"),
        F.col("l_commitdate").alias("commit_date"),
        F.col("l_receiptdate").alias("receipt_date"),
        F.col("l_quantity").alias("quantity"),
        F.col("l_extendedprice").alias("extended_price"),
        F.col("l_discount").alias("discount"),
        F.col("l_tax").alias("tax"),
        F.col("l_returnflag").alias("return_flag"),
        F.col("l_linestatus").alias("line_status")
    )

    return (
        df.withColumn("return_flag", F.when(F.trim(F.col("return_flag")) == "", F.lit(None)).otherwise(F.col("return_flag")))
          .withColumn("line_status", F.when(F.trim(F.col("line_status")) == "", F.lit(None)).otherwise(F.col("line_status")))
          .withColumn("shipping_delay_days", F.datediff(F.col("receipt_date"), F.col("commit_date")))
          .withColumn("is_on_time_shipment", F.when(F.col("receipt_date") <= F.col("commit_date"), F.lit(1)).otherwise(F.lit(0)))
          .withColumn("is_late_shipment", F.when(F.col("receipt_date") > F.col("commit_date"), F.lit(1)).otherwise(F.lit(0)))
          .withColumn("gross_revenue", F.col("extended_price"))
          .withColumn("net_revenue", F.col("extended_price") * (F.lit(1.0) - F.col("discount")))
    )

# COMMAND ----------
@dlt.table(
    name="orders_silver",
    comment="Cleaned orders records."
)
@dlt.expect("valid_orderkey", "order_key IS NOT NULL")
def orders_silver():
    df = spark.read.table("workspace.bronze.orders_bronze").select(
        F.col("o_orderkey").alias("order_key"),
        F.col("o_orderdate").alias("order_date"),
        F.col("o_orderstatus").alias("order_status"),
        F.col("o_totalprice").alias("order_total_price"),
        F.col("o_orderpriority").alias("order_priority")
    )

    return (
        df.withColumn("order_status", F.when(F.trim(F.col("order_status")) == "", F.lit(None)).otherwise(F.col("order_status")))
          .withColumn("order_priority", F.when(F.trim(F.col("order_priority")) == "", F.lit(None)).otherwise(F.col("order_priority")))
    )

# COMMAND ----------
@dlt.table(
    name="supplier_silver",
    comment="Cleaned supplier records."
)
@dlt.expect("valid_supplierkey", "supplier_key IS NOT NULL")
@dlt.expect("valid_nationkey", "nation_key IS NOT NULL")
def supplier_silver():
    df = spark.read.table("workspace.bronze.supplier_bronze").select(
        F.col("s_suppkey").alias("supplier_key"),
        F.col("s_nationkey").alias("nation_key"),
        F.col("s_name").alias("supplier_name"),
        F.col("s_address").alias("supplier_address"),
        F.col("s_phone").alias("supplier_phone"),
        F.col("s_acctbal").alias("supplier_acctbal"),
        F.col("s_comment").alias("supplier_comment")
    )

    return (
        df.withColumn("supplier_name", F.when(F.trim(F.col("supplier_name")) == "", F.lit(None)).otherwise(F.col("supplier_name")))
          .withColumn("supplier_address", F.when(F.trim(F.col("supplier_address")) == "", F.lit(None)).otherwise(F.col("supplier_address")))
          .withColumn("supplier_phone", F.when(F.trim(F.col("supplier_phone")) == "", F.lit(None)).otherwise(F.col("supplier_phone")))
          .withColumn("supplier_comment", F.when(F.trim(F.col("supplier_comment")) == "", F.lit(None)).otherwise(F.col("supplier_comment")))
    )

# COMMAND ----------
@dlt.table(
    name="nation_silver",
    comment="Cleaned nation records."
)
@dlt.expect("valid_nationkey", "nation_key IS NOT NULL")
@dlt.expect("valid_regionkey", "region_key IS NOT NULL")
def nation_silver():
    df = spark.read.table("workspace.bronze.nation_bronze").select(
        F.col("n_nationkey").alias("nation_key"),
        F.col("n_regionkey").alias("region_key"),
        F.col("n_name").alias("nation_name"),
        F.col("n_comment").alias("nation_comment")
    )

    return (
        df.withColumn("nation_name", F.when(F.trim(F.col("nation_name")) == "", F.lit(None)).otherwise(F.col("nation_name")))
          .withColumn("nation_comment", F.when(F.trim(F.col("nation_comment")) == "", F.lit(None)).otherwise(F.col("nation_comment")))
    )

# COMMAND ----------
@dlt.table(
    name="region_silver",
    comment="Cleaned region records."
)
@dlt.expect("valid_regionkey", "region_key IS NOT NULL")
def region_silver():
    df = spark.read.table("workspace.bronze.region_bronze").select(
        F.col("r_regionkey").alias("region_key"),
        F.col("r_name").alias("region_name"),
        F.col("r_comment").alias("region_comment")
    )

    return (
        df.withColumn("region_name", F.when(F.trim(F.col("region_name")) == "", F.lit(None)).otherwise(F.col("region_name")))
          .withColumn("region_comment", F.when(F.trim(F.col("region_comment")) == "", F.lit(None)).otherwise(F.col("region_comment")))
    )

# COMMAND ----------
@dlt.table(
    name="supplier_geo_silver",
    comment="Supplier with nation and region attributes."
)
@dlt.expect("supplier_with_geo_keys", "supplier_key IS NOT NULL AND nation_key IS NOT NULL AND region_key IS NOT NULL")
def supplier_geo_silver():
    s = dlt.read("supplier_silver")
    n = dlt.read("nation_silver")
    r = dlt.read("region_silver")

    return (
        s.join(n, on="nation_key", how="left")
         .join(r, on="region_key", how="left")
         .select(
             "supplier_key",
             "nation_key",
             "region_key",
             "supplier_name",
             "nation_name",
             "region_name"
         )
    )

# COMMAND ----------
@dlt.table(
    name="lineitem_order_validated_silver",
    comment="Lineitems validated against existing orders."
)
@dlt.expect_or_drop("lineitem_matches_order", "order_key IS NOT NULL")
@dlt.expect("non_negative_delay_floor", "shipping_delay_days >= -36500")
def lineitem_order_validated_silver():
    l = dlt.read("lineitem_silver")
    o = dlt.read("orders_silver")

    return (
        l.join(o, on="order_key", how="inner")
         .select(
             "order_key",
             "supplier_key",
             "ship_date",
             "commit_date",
             "receipt_date",
             "quantity",
             "extended_price",
             "discount",
             "tax",
             "shipping_delay_days",
             "is_on_time_shipment",
             "is_late_shipment",
             "gross_revenue",
             "net_revenue",
             "order_date",
             "order_status",
             "order_total_price",
             "order_priority"
         )
    )