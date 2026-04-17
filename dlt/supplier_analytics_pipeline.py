# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
# Bronze Layer: Raw ingestion from samples.tpch with standardization and audit columns

@dlt.table(
    name="workspace.bronze.lineitem_bronze",
    comment="Bronze lineitem data from samples.tpch.lineitem with standardized column names and audit metadata"
)
def lineitem_bronze():
    return (
        spark.table("samples.tpch.lineitem")
        .select(
            F.col("l_orderkey").alias("order_key"),
            F.col("l_partkey").alias("part_key"),
            F.col("l_suppkey").alias("supplier_key"),
            F.col("l_linenumber").alias("line_number"),
            F.col("l_quantity").alias("quantity"),
            F.col("l_extendedprice").alias("extended_price"),
            F.col("l_discount").alias("discount"),
            F.col("l_tax").alias("tax"),
            F.col("l_returnflag").alias("return_flag"),
            F.col("l_linestatus").alias("line_status"),
            F.col("l_shipdate").alias("ship_date_raw"),
            F.col("l_commitdate").alias("commit_date_raw"),
            F.col("l_receiptdate").alias("receipt_date_raw"),
            F.col("l_shipinstruct").alias("ship_instruct"),
            F.col("l_shipmode").alias("ship_mode"),
            F.col("l_comment").alias("line_comment")
        )
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_system", F.lit("samples.tpch.lineitem"))
    )

@dlt.table(
    name="workspace.bronze.supplier_bronze",
    comment="Bronze supplier data from samples.tpch.supplier with standardized column names and audit metadata"
)
def supplier_bronze():
    return (
        spark.table("samples.tpch.supplier")
        .select(
            F.col("s_suppkey").alias("supplier_key"),
            F.col("s_name").alias("supplier_name"),
            F.col("s_address").alias("supplier_address"),
            F.col("s_nationkey").alias("nation_key"),
            F.col("s_phone").alias("supplier_phone"),
            F.col("s_acctbal").alias("account_balance"),
            F.col("s_comment").alias("supplier_comment")
        )
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_system", F.lit("samples.tpch.supplier"))
    )

@dlt.table(
    name="workspace.bronze.orders_bronze",
    comment="Bronze orders data from samples.tpch.orders with standardized column names and audit metadata"
)
def orders_bronze():
    return (
        spark.table("samples.tpch.orders")
        .select(
            F.col("o_orderkey").alias("order_key"),
            F.col("o_custkey").alias("customer_key"),
            F.col("o_orderstatus").alias("order_status"),
            F.col("o_totalprice").alias("order_total_price"),
            F.col("o_orderdate").alias("order_date"),
            F.col("o_orderpriority").alias("order_priority"),
            F.col("o_clerk").alias("clerk"),
            F.col("o_shippriority").alias("ship_priority"),
            F.col("o_comment").alias("order_comment")
        )
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_system", F.lit("samples.tpch.orders"))
    )

@dlt.table(
    name="workspace.bronze.nation_bronze",
    comment="Bronze nation data from samples.tpch.nation with standardized column names and audit metadata"
)
def nation_bronze():
    return (
        spark.table("samples.tpch.nation")
        .select(
            F.col("n_nationkey").alias("nation_key"),
            F.col("n_name").alias("nation_name"),
            F.col("n_regionkey").alias("region_key"),
            F.col("n_comment").alias("nation_comment")
        )
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_system", F.lit("samples.tpch.nation"))
    )

@dlt.table(
    name="workspace.bronze.region_bronze",
    comment="Bronze region data from samples.tpch.region with standardized column names and audit metadata"
)
def region_bronze():
    return (
        spark.table("samples.tpch.region")
        .select(
            F.col("r_regionkey").alias("region_key"),
            F.col("r_name").alias("region_name"),
            F.col("r_comment").alias("region_comment")
        )
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("source_system", F.lit("samples.tpch.region"))
    )

# COMMAND ----------
# Silver Layer: Data cleaning, expectations, enrichment, and conformance

@dlt.table(
    name="workspace.silver.lineitem_silver",
    comment="Cleaned and conformed lineitem data with shipment and revenue metrics"
)
@dlt.expect_or_drop("valid_order_key", "order_key IS NOT NULL")
@dlt.expect_or_drop("valid_supplier_key", "supplier_key IS NOT NULL")
@dlt.expect_or_drop("valid_quantity_non_negative", "quantity >= 0")
@dlt.expect_or_drop("valid_extended_price_non_negative", "extended_price >= 0")
@dlt.expect_or_drop("valid_discount_range", "discount >= 0 AND discount <= 1")
@dlt.expect_or_drop("valid_tax_range", "tax >= 0 AND tax <= 1")
@dlt.expect_or_drop("valid_date_ordering", "receipt_date >= commit_date")
def lineitem_silver():
    bronze_df = dlt.read("workspace.bronze.lineitem_bronze")
    return (
        bronze_df
        .withColumn("ship_date", F.to_date("ship_date_raw"))
        .withColumn("commit_date", F.to_date("commit_date_raw"))
        .withColumn("receipt_date", F.to_date("receipt_date_raw"))
        .withColumn("gross_revenue", F.col("extended_price"))
        .withColumn("net_revenue", F.col("extended_price") * (F.lit(1.0) - F.col("discount")))
        .withColumn("is_on_time", F.when(F.col("receipt_date") <= F.col("commit_date"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("is_late", F.when(F.col("receipt_date") > F.col("commit_date"), F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "ship_delay_days",
            F.when(F.col("receipt_date") > F.col("commit_date"), F.datediff(F.col("receipt_date"), F.col("commit_date"))).otherwise(F.lit(0))
        )
        .withColumn("ship_month", F.date_trunc("month", F.col("ship_date")).cast("date"))
        .drop("ship_date_raw", "commit_date_raw", "receipt_date_raw")
    )

@dlt.table(
    name="workspace.silver.supplier_enriched_silver",
    comment="Supplier dimension enriched with nation and region attributes"
)
@dlt.expect_or_drop("valid_supplier_key_not_null", "supplier_key IS NOT NULL")
@dlt.expect_or_drop("valid_nation_key_not_null", "nation_key IS NOT NULL")
def supplier_enriched_silver():
    supplier_df = dlt.read("workspace.bronze.supplier_bronze")
    nation_df = dlt.read("workspace.bronze.nation_bronze")
    region_df = dlt.read("workspace.bronze.region_bronze")

    return (
        supplier_df.alias("s")
        .join(nation_df.alias("n"), F.col("s.nation_key") == F.col("n.nation_key"), "left")
        .join(region_df.alias("r"), F.col("n.region_key") == F.col("r.region_key"), "left")
        .select(
            F.col("s.supplier_key"),
            F.col("s.supplier_name"),
            F.col("s.supplier_address"),
            F.col("s.nation_key"),
            F.col("n.nation_name"),
            F.col("n.region_key"),
            F.col("r.region_name"),
            F.col("s.supplier_phone"),
            F.col("s.account_balance"),
            F.col("s.supplier_comment"),
            F.col("s.ingestion_ts").alias("supplier_ingestion_ts")
        )
    )

@dlt.table(
    name="workspace.silver.supplier_order_lineitem_silver",
    comment="Conformed supplier-order-lineitem fact base for KPI aggregation"
)
@dlt.expect_or_drop("valid_joined_supplier_key", "supplier_key IS NOT NULL")
@dlt.expect_or_drop("valid_joined_order_key", "order_key IS NOT NULL")
@dlt.expect_or_drop("valid_ship_date_not_null", "ship_date IS NOT NULL")
def supplier_order_lineitem_silver():
    lineitem_df = dlt.read("workspace.silver.lineitem_silver")
    supplier_enriched_df = dlt.read("workspace.silver.supplier_enriched_silver")
    orders_df = dlt.read("workspace.bronze.orders_bronze")

    return (
        lineitem_df.alias("l")
        .join(supplier_enriched_df.alias("s"), F.col("l.supplier_key") == F.col("s.supplier_key"), "inner")
        .join(orders_df.alias("o"), F.col("l.order_key") == F.col("o.order_key"), "inner")
        .select(
            F.col("l.supplier_key"),
            F.col("l.order_key"),
            F.col("l.ship_date"),
            F.col("s.nation_key"),
            F.col("s.region_key"),
            F.col("l.line_number"),
            F.col("l.quantity"),
            F.col("l.extended_price"),
            F.col("l.discount"),
            F.col("l.tax"),
            F.col("l.gross_revenue"),
            F.col("l.net_revenue"),
            F.col("l.is_on_time"),
            F.col("l.is_late"),
            F.col("l.ship_delay_days"),
            F.col("l.ship_month")
        )
    )

# COMMAND ----------
# Gold Layer: Business-ready supplier analytics KPIs

@dlt.table(
    name="workspace.gold.supplier_order_ship_kpi_gold",
    comment="Supplier-order-ship-date KPI aggregate for decision support dashboard"
)
def supplier_order_ship_kpi_gold():
    base_df = dlt.read("workspace.silver.supplier_order_lineitem_silver")

    return (
        base_df
        .groupBy("supplier_key", "order_key", "ship_date", "nation_key", "region_key")
        .agg(
            F.count(F.lit(1)).alias("total_line_items"),
            F.countDistinct("order_key").alias("total_orders"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("gross_revenue").alias("gross_revenue"),
            F.sum("net_revenue").alias("net_revenue"),
            F.avg("discount").alias("average_discount"),
            F.avg("tax").alias("average_tax"),
            F.sum("is_on_time").alias("on_time_shipments"),
            F.sum("is_late").alias("late_shipments"),
            F.avg("ship_delay_days").alias("average_ship_delay"),
            F.max("ship_delay_days").alias("maximum_ship_delay")
        )
    )