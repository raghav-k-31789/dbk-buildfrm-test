# Databricks notebook source

# Bronze layer: imports and shared configuration for DLT pipeline
import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------
# Bronze layer: lineitem standardized materialized view with audit columns
@dlt.table(
    name="lineitem_bronze",
    comment="Bronze lineitem from samples.tpch.lineitem with light standardization and audit metadata."
)
def lineitem_bronze():
    return (
        spark.table("samples.tpch.lineitem")
        .select(
            F.col("l_orderkey").cast("long").alias("order_key"),
            F.col("l_partkey").cast("long").alias("part_key"),
            F.col("l_suppkey").cast("long").alias("supplier_key"),
            F.col("l_linenumber").cast("int").alias("line_number"),
            F.col("l_quantity").cast("double").alias("quantity"),
            F.col("l_extendedprice").cast("double").alias("extended_price"),
            F.col("l_discount").cast("double").alias("discount"),
            F.col("l_tax").cast("double").alias("tax"),
            F.to_date("l_shipdate").alias("ship_date"),
            F.to_date("l_commitdate").alias("commit_date"),
            F.to_date("l_receiptdate").alias("receipt_date"),
            F.col("l_returnflag").alias("return_flag"),
            F.col("l_linestatus").alias("line_status"),
            F.col("l_shipinstruct").alias("ship_instruct"),
            F.col("l_shipmode").alias("ship_mode"),
            F.current_timestamp().alias("_ingest_ts"),
            F.lit("samples.tpch.lineitem").alias("_source_table")
        )
    )

# COMMAND ----------
# Bronze layer: supplier standardized materialized view with audit columns
@dlt.table(
    name="supplier_bronze",
    comment="Bronze supplier from samples.tpch.supplier with light standardization and audit metadata."
)
def supplier_bronze():
    return (
        spark.table("samples.tpch.supplier")
        .select(
            F.col("s_suppkey").cast("long").alias("supplier_key"),
            F.col("s_name").alias("supplier_name"),
            F.col("s_address").alias("supplier_address"),
            F.col("s_nationkey").cast("long").alias("nation_key"),
            F.col("s_phone").alias("supplier_phone"),
            F.col("s_acctbal").cast("double").alias("account_balance"),
            F.col("s_comment").alias("supplier_comment"),
            F.current_timestamp().alias("_ingest_ts"),
            F.lit("samples.tpch.supplier").alias("_source_table")
        )
    )

# COMMAND ----------
# Bronze layer: orders standardized materialized view with audit columns
@dlt.table(
    name="orders_bronze",
    comment="Bronze orders from samples.tpch.orders with light standardization and audit metadata."
)
def orders_bronze():
    return (
        spark.table("samples.tpch.orders")
        .select(
            F.col("o_orderkey").cast("long").alias("order_key"),
            F.col("o_custkey").cast("long").alias("customer_key"),
            F.col("o_orderstatus").alias("order_status"),
            F.col("o_totalprice").cast("double").alias("order_total_price"),
            F.to_date("o_orderdate").alias("order_date"),
            F.col("o_orderpriority").alias("order_priority"),
            F.col("o_clerk").alias("clerk"),
            F.col("o_shippriority").cast("int").alias("ship_priority"),
            F.col("o_comment").alias("order_comment"),
            F.current_timestamp().alias("_ingest_ts"),
            F.lit("samples.tpch.orders").alias("_source_table")
        )
    )

# COMMAND ----------
# Bronze layer: nation standardized materialized view with audit columns
@dlt.table(
    name="nation_bronze",
    comment="Bronze nation from samples.tpch.nation with light standardization and audit metadata."
)
def nation_bronze():
    return (
        spark.table("samples.tpch.nation")
        .select(
            F.col("n_nationkey").cast("long").alias("nation_key"),
            F.col("n_name").alias("nation_name"),
            F.col("n_regionkey").cast("long").alias("region_key"),
            F.col("n_comment").alias("nation_comment"),
            F.current_timestamp().alias("_ingest_ts"),
            F.lit("samples.tpch.nation").alias("_source_table")
        )
    )

# COMMAND ----------
# Bronze layer: region standardized materialized view with audit columns
@dlt.table(
    name="region_bronze",
    comment="Bronze region from samples.tpch.region with light standardization and audit metadata."
)
def region_bronze():
    return (
        spark.table("samples.tpch.region")
        .select(
            F.col("r_regionkey").cast("long").alias("region_key"),
            F.col("r_name").alias("region_name"),
            F.col("r_comment").alias("region_comment"),
            F.current_timestamp().alias("_ingest_ts"),
            F.lit("samples.tpch.region").alias("_source_table")
        )
    )

# COMMAND ----------
# Silver layer: conformed supplier-nation-region dimension with valid join keys
@dlt.table(
    name="supplier_enriched_silver",
    comment="Conformed supplier dimension joined with nation and region."
)
@dlt.expect_all_or_drop({
    "supplier_key_not_null": "supplier_key IS NOT NULL",
    "nation_key_not_null": "nation_key IS NOT NULL",
    "region_key_not_null": "region_key IS NOT NULL"
})
def supplier_enriched_silver():
    s = dlt.read("supplier_bronze").alias("s")
    n = dlt.read("nation_bronze").alias("n")
    r = dlt.read("region_bronze").alias("r")

    return (
        s.join(n, F.col("s.nation_key") == F.col("n.nation_key"), "inner")
         .join(r, F.col("n.region_key") == F.col("r.region_key"), "inner")
         .select(
             F.col("s.supplier_key"),
             F.col("s.supplier_name"),
             F.col("s.supplier_address"),
             F.col("s.nation_key"),
             F.col("n.nation_name"),
             F.col("n.region_key"),
             F.col("r.region_name"),
             F.col("s.supplier_phone"),
             F.col("s.account_balance")
         )
    )

# COMMAND ----------
# Silver layer: cleaned lineitem facts with type checks, value checks, and ship delay derivations
@dlt.table(
    name="lineitem_clean_silver",
    comment="Cleaned lineitem facts with quality constraints, revenue derivations, and shipment delay metrics."
)
@dlt.expect_all_or_drop({
    "order_key_not_null": "order_key IS NOT NULL",
    "supplier_key_not_null": "supplier_key IS NOT NULL",
    "quantity_non_negative": "quantity >= 0",
    "extended_price_non_negative": "extended_price >= 0",
    "discount_in_range": "discount >= 0 AND discount <= 1",
    "tax_in_range": "tax >= 0 AND tax <= 1",
    "ship_date_not_null": "ship_date IS NOT NULL",
    "commit_date_not_null": "commit_date IS NOT NULL",
    "receipt_date_not_null": "receipt_date IS NOT NULL",
    "ship_before_or_on_receipt": "ship_date <= receipt_date"
})
def lineitem_clean_silver():
    li = dlt.read("lineitem_bronze")
    return (
        li.withColumn("gross_revenue", F.col("extended_price"))
          .withColumn("net_revenue", F.col("extended_price") * (F.lit(1.0) - F.col("discount")))
          .withColumn("ship_delay_days", F.greatest(F.datediff(F.col("receipt_date"), F.col("commit_date")), F.lit(0)))
          .withColumn("is_on_time", F.when(F.col("receipt_date") <= F.col("commit_date"), F.lit(1)).otherwise(F.lit(0)))
          .withColumn("is_late", F.when(F.col("receipt_date") > F.col("commit_date"), F.lit(1)).otherwise(F.lit(0)))
          .withColumn("ship_month", F.date_trunc("month", F.col("ship_date")).cast("date"))
    )

# COMMAND ----------
# Silver layer: lineitem facts conformed with supplier geography and order context
@dlt.table(
    name="supplier_order_lineitem_silver",
    comment="Conformed supplier-order-lineitem facts for KPI aggregation."
)
@dlt.expect_all_or_drop({
    "valid_supplier_join": "supplier_key IS NOT NULL AND nation_key IS NOT NULL AND region_key IS NOT NULL",
    "valid_order_join": "order_key IS NOT NULL"
})
def supplier_order_lineitem_silver():
    li = dlt.read("lineitem_clean_silver").alias("li")
    se = dlt.read("supplier_enriched_silver").alias("se")
    o = dlt.read("orders_bronze").alias("o")

    return (
        li.join(se, F.col("li.supplier_key") == F.col("se.supplier_key"), "inner")
          .join(o, F.col("li.order_key") == F.col("o.order_key"), "inner")
          .select(
              F.col("li.supplier_key"),
              F.col("li.order_key"),
              F.col("li.ship_date"),
              F.col("se.nation_key"),
              F.col("se.region_key"),
              F.col("li.quantity"),
              F.col("li.gross_revenue"),
              F.col("li.net_revenue"),
              F.col("li.discount"),
              F.col("li.tax"),
              F.col("li.is_on_time"),
              F.col("li.is_late"),
              F.col("li.ship_delay_days"),
              F.col("li.ship_month")
          )
    )

# COMMAND ----------
# Gold layer: final KPI table at grain supplier_key + order_key + ship_date for dashboarding
@dlt.table(
    name="supplier_order_ship_kpi_gold",
    comment="Final supplier analytics KPI table at grain supplier_key + order_key + ship_date."
)
@dlt.expect_all_or_drop({
    "gold_supplier_key_not_null": "supplier_key IS NOT NULL",
    "gold_order_key_not_null": "order_key IS NOT NULL",
    "gold_ship_date_not_null": "ship_date IS NOT NULL"
})
def supplier_order_ship_kpi_gold():
    f = dlt.read("supplier_order_lineitem_silver")
    return (
        f.groupBy("supplier_key", "order_key", "ship_date", "nation_key", "region_key")
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

# COMMAND ----------
# Validation output removed for production pipeline deployment
