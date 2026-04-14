# Databricks notebook source
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
@dlt.view(name="lineitem_src")
def lineitem_src():
    return spark.table("tpch.lineitem")

@dlt.view(name="supplier_src")
def supplier_src():
    return spark.table("tpch.supplier")

@dlt.view(name="nation_src")
def nation_src():
    return spark.table("tpch.nation")

@dlt.view(name="region_src")
def region_src():
    return spark.table("tpch.region")

@dlt.view(name="orders_src")
def orders_src():
    return spark.table("tpch.orders")

# COMMAND ----------
@dlt.view(name="supplier_analytics")
def supplier_analytics():
    li = dlt.read("lineitem_src").alias("li")
    s = dlt.read("supplier_src").alias("s")
    n = dlt.read("nation_src").alias("n")
    r = dlt.read("region_src").alias("r")
    o = dlt.read("orders_src").alias("o")

    aggregated = (
        li.join(s, F.col("li.l_suppkey") == F.col("s.s_suppkey"), "inner")
          .join(n, F.col("s.s_nationkey") == F.col("n.n_nationkey"), "left")
          .join(r, F.col("n.n_regionkey") == F.col("r.r_regionkey"), "left")
          .join(o, F.col("li.l_orderkey") == F.col("o.o_orderkey"), "left")
          .groupBy(
              F.col("li.l_suppkey"),
              F.col("li.l_orderkey"),
              F.col("li.l_shipdate"),
              F.col("s.s_nationkey"),
              F.col("n.n_regionkey")
          )
          .agg(
              F.count(F.lit(1)).alias("total_line_items"),
              F.countDistinct(F.col("li.l_orderkey")).alias("total_orders"),
              F.sum(F.col("li.l_quantity")).alias("total_quantity"),
              F.sum(F.col("li.l_extendedprice")).alias("gross_revenue"),
              F.sum(F.col("li.l_extendedprice") * (F.lit(1) - F.col("li.l_discount"))).alias("net_revenue"),
              F.avg(F.col("li.l_discount")).alias("average_discount"),
              F.avg(F.col("li.l_tax")).alias("average_tax"),
              F.sum(F.when(F.col("li.l_receiptdate") <= F.col("li.l_commitdate"), F.lit(1)).otherwise(F.lit(0))).alias("on_time_shipments"),
              F.sum(F.when(F.col("li.l_receiptdate") > F.col("li.l_commitdate"), F.lit(1)).otherwise(F.lit(0))).alias("late_shipments"),
              F.avg(F.datediff(F.col("li.l_receiptdate"), F.col("li.l_commitdate"))).alias("average_ship_delay_days"),
              F.max(F.datediff(F.col("li.l_receiptdate"), F.col("li.l_commitdate"))).alias("maximum_ship_delay_days")
          )
          .select(
              F.col("l_suppkey").alias("supplier_key"),
              F.col("l_orderkey").alias("order_key"),
              F.col("l_shipdate").alias("ship_date_key"),
              F.col("s_nationkey").alias("nation_key"),
              F.col("n_regionkey").alias("region_key"),
              F.col("total_line_items"),
              F.col("total_orders"),
              F.col("total_quantity"),
              F.round(F.col("gross_revenue"), 2).alias("gross_revenue"),
              F.round(F.col("net_revenue"), 2).alias("net_revenue"),
              F.round(F.col("average_discount"), 4).alias("average_discount"),
              F.round(F.col("average_tax"), 4).alias("average_tax"),
              F.col("on_time_shipments"),
              F.col("late_shipments"),
              F.round(F.col("average_ship_delay_days"), 2).alias("average_ship_delay_days"),
              F.col("maximum_ship_delay_days")
          )
          .orderBy("supplier_key", "order_key", "ship_date_key")
    )

    return aggregated