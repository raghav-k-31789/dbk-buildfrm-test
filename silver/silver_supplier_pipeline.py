# Databricks notebook source
# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
@dlt.table(
    name="workspace.silver.lineitem_silver",
    comment="Silver lineitem with standardized types, null/blank normalization, and derived shipment/revenue fields."
)
@dlt.expect("valid_orderkey", "order_key IS NOT NULL AND order_key > 0")
@dlt.expect("valid_suppkey", "supplier_key IS NOT NULL AND supplier_key > 0")
@dlt.expect("valid_shipdate", "ship_date IS NOT NULL")
@dlt.expect("valid_numeric_ranges", "quantity >= 0 AND extended_price >= 0 AND discount BETWEEN 0 AND 1 AND tax BETWEEN 0 AND 1")
def lineitem_silver():
    return (
        spark.read.table("workspace.bronze.lineitem_bronze")
        .select(
            F.col("l_orderkey").cast("bigint").alias("order_key"),
            F.col("l_suppkey").cast("bigint").alias("supplier_key"),
            F.to_date("l_shipdate").alias("ship_date"),
            F.to_date("l_commitdate").alias("commit_date"),
            F.to_date("l_receiptdate").alias("receipt_date"),
            F.col("l_quantity").cast("double").alias("quantity"),
            F.col("l_extendedprice").cast("double").alias("extended_price"),
            F.col("l_discount").cast("double").alias("discount"),
            F.col("l_tax").cast("double").alias("tax"),
            F.when(F.trim(F.col("l_returnflag")) == "", None).otherwise(F.trim(F.col("l_returnflag"))).alias("return_flag"),
            F.when(F.trim(F.col("l_linestatus")) == "", None).otherwise(F.trim(F.col("l_linestatus"))).alias("line_status")
        )
        .withColumn("shipping_delay_days", F.datediff(F.col("receipt_date"), F.col("commit_date")))
        .withColumn("is_on_time_shipment", F.when(F.col("receipt_date") <= F.col("commit_date"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("is_late_shipment", F.when(F.col("receipt_date") > F.col("commit_date"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("gross_revenue", F.col("extended_price"))
        .withColumn("net_revenue", F.col("extended_price") * (F.lit(1.0) - F.col("discount")))
    )

# COMMAND ----------
@dlt.table(
    name="workspace.silver.orders_silver",
    comment="Silver orders with standardized keys and cleaned order status."
)
@dlt.expect("valid_orderkey", "order_key IS NOT NULL AND order_key > 0")
def orders_silver():
    return (
        spark.read.table("workspace.bronze.orders_bronze")
        .select(
            F.col("o_orderkey").cast("bigint").alias("order_key"),
            F.to_date("o_orderdate").alias("order_date"),
            F.when(F.trim(F.col("o_orderstatus")) == "", None).otherwise(F.trim(F.col("o_orderstatus"))).alias("order_status"),
            F.col("o_totalprice").cast("double").alias("order_total_price"),
            F.when(F.trim(F.col("o_orderpriority")) == "", None).otherwise(F.trim(F.col("o_orderpriority"))).alias("order_priority")
        )
    )

# COMMAND ----------
@dlt.table(
    name="workspace.silver.supplier_silver",
    comment="Silver supplier with standardized keys and cleaned descriptive attributes."
)
@dlt.expect("valid_supplierkey", "supplier_key IS NOT NULL AND supplier_key > 0")
@dlt.expect("valid_nationkey", "nation_key IS NOT NULL AND nation_key >= 0")
def supplier_silver():
    return (
        spark.read.table("workspace.bronze.supplier_bronze")
        .select(
            F.col("s_suppkey").cast("bigint").alias("supplier_key"),
            F.col("s_nationkey").cast("bigint").alias("nation_key"),
            F.when(F.trim(F.col("s_name")) == "", None).otherwise(F.trim(F.col("s_name"))).alias("supplier_name"),
            F.when(F.trim(F.col("s_address")) == "", None).otherwise(F.trim(F.col("s_address"))).alias("supplier_address"),
            F.when(F.trim(F.col("s_phone")) == "", None).otherwise(F.trim(F.col("s_phone"))).alias("supplier_phone"),
            F.col("s_acctbal").cast("double").alias("supplier_acctbal"),
            F.when(F.trim(F.col("s_comment")) == "", None).otherwise(F.trim(F.col("s_comment"))).alias("supplier_comment")
        )
    )

# COMMAND ----------
@dlt.table(
    name="workspace.silver.nation_silver",
    comment="Silver nation with standardized keys and cleaned nation names."
)
@dlt.expect("valid_nationkey", "nation_key IS NOT NULL AND nation_key >= 0")
@dlt.expect("valid_regionkey", "region_key IS NOT NULL AND region_key >= 0")
def nation_silver():
    return (
        spark.read.table("workspace.bronze.nation_bronze")
        .select(
            F.col("n_nationkey").cast("bigint").alias("nation_key"),
            F.col("n_regionkey").cast("bigint").alias("region_key"),
            F.when(F.trim(F.col("n_name")) == "", None).otherwise(F.trim(F.col("n_name"))).alias("nation_name"),
            F.when(F.trim(F.col("n_comment")) == "", None).otherwise(F.trim(F.col("n_comment"))).alias("nation_comment")
        )
    )

# COMMAND ----------
@dlt.table(
    name="workspace.silver.region_silver",
    comment="Silver region with standardized keys and cleaned region names."
)
@dlt.expect("valid_regionkey", "region_key IS NOT NULL AND region_key >= 0")
def region_silver():
    return (
        spark.read.table("workspace.bronze.region_bronze")
        .select(
            F.col("r_regionkey").cast("bigint").alias("region_key"),
            F.when(F.trim(F.col("r_name")) == "", None).otherwise(F.trim(F.col("r_name"))).alias("region_name"),
            F.when(F.trim(F.col("r_comment")) == "", None).otherwise(F.trim(F.col("r_comment"))).alias("region_comment")
        )
    )

# COMMAND ----------
@dlt.table(
    name="workspace.silver.supplier_geo_silver",
    comment="Conformed supplier-to-nation-to-region mapping."
)
@dlt.expect("supplier_with_geo_keys", "supplier_key IS NOT NULL AND nation_key IS NOT NULL AND region_key IS NOT NULL")
def supplier_geo_silver():
    s = dlt.read("workspace.silver.supplier_silver").alias("s")
    n = dlt.read("workspace.silver.nation_silver").alias("n")
    r = dlt.read("workspace.silver.region_silver").alias("r")

    return (
        s.join(n, F.col("s.nation_key") == F.col("n.nation_key"), "left")
         .join(r, F.col("n.region_key") == F.col("r.region_key"), "left")
         .select(
             F.col("s.supplier_key"),
             F.col("s.nation_key"),
             F.col("n.region_key"),
             F.col("s.supplier_name"),
             F.col("n.nation_name"),
             F.col("r.region_name")
         )
    )

# COMMAND ----------
@dlt.table(
    name="workspace.silver.lineitem_order_validated_silver",
    comment="Validated lineitem-orders join for downstream gold aggregation."
)
@dlt.expect_or_drop("lineitem_matches_order", "order_key IS NOT NULL")
@dlt.expect("non_negative_delay_floor", "shipping_delay_days >= -36500")
def lineitem_order_validated_silver():
    li = dlt.read("workspace.silver.lineitem_silver").alias("li")
    o = dlt.read("workspace.silver.orders_silver").alias("o")

    return (
        li.join(o, F.col("li.order_key") == F.col("o.order_key"), "inner")
          .select(
              F.col("li.order_key"),
              F.col("li.supplier_key"),
              F.col("li.ship_date"),
              F.col("li.commit_date"),
              F.col("li.receipt_date"),
              F.col("li.quantity"),
              F.col("li.extended_price"),
              F.col("li.discount"),
              F.col("li.tax"),
              F.col("li.shipping_delay_days"),
              F.col("li.is_on_time_shipment"),
              F.col("li.is_late_shipment"),
              F.col("li.gross_revenue"),
              F.col("li.net_revenue"),
              F.col("o.order_date"),
              F.col("o.order_status"),
              F.col("o.order_total_price"),
              F.col("o.order_priority")
          )
    )

# COMMAND ----------
display(spark.read.table("workspace.silver.lineitem_order_validated_silver"))