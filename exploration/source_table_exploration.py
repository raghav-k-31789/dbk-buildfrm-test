# Databricks notebook source

# Load required PySpark functions and set source table references for exploration
from pyspark.sql import functions as F

tables = {
    "lineitem": "samples.tpch.lineitem",
    "supplier": "samples.tpch.supplier",
    "nation": "samples.tpch.nation",
    "region": "samples.tpch.region",
    "orders": "samples.tpch.orders",
}

# COMMAND ----------
# Load source tables into DataFrames for profiling and relationship checks
lineitem_df = spark.table(tables["lineitem"])
supplier_df = spark.table(tables["supplier"])
nation_df = spark.table(tables["nation"])
region_df = spark.table(tables["region"])
orders_df = spark.table(tables["orders"])

# COMMAND ----------
# Inspect schemas to validate expected columns used in the SQL transformation
print("lineitem schema")
lineitem_df.printSchema()

print("supplier schema")
supplier_df.printSchema()

print("nation schema")
nation_df.printSchema()

print("region schema")
region_df.printSchema()

print("orders schema")
orders_df.printSchema()

# COMMAND ----------
# Compute row counts across all source tables to understand table sizes
row_counts_df = spark.createDataFrame(
    [(name, spark.table(tbl).count()) for name, tbl in tables.items()],
    ["table_name", "row_count"]
).orderBy("table_name")

display(row_counts_df)

# COMMAND ----------
# Profile null counts for lineitem columns relevant to metrics and joins
lineitem_nulls_df = lineitem_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) 
    for c in [
        "l_orderkey", "l_suppkey", "l_shipdate", "l_commitdate", "l_receiptdate",
        "l_quantity", "l_extendedprice", "l_discount", "l_tax"
    ]
])

display(lineitem_nulls_df)

# COMMAND ----------
# Profile null counts for supplier, nation, region, and orders join keys
supplier_nulls_df = supplier_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ["s_suppkey", "s_nationkey"]
])

nation_nulls_df = nation_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ["n_nationkey", "n_regionkey"]
])

region_nulls_df = region_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ["r_regionkey"]
])

orders_nulls_df = orders_df.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ["o_orderkey"]
])

print("supplier null profile")
display(supplier_nulls_df)

print("nation null profile")
display(nation_nulls_df)

print("region null profile")
display(region_nulls_df)

print("orders null profile")
display(orders_nulls_df)

# COMMAND ----------
# Preview source records to validate data shape and content
print("lineitem preview")
display(lineitem_df.limit(20))

print("supplier preview")
display(supplier_df.limit(20))

print("nation preview")
display(nation_df.limit(20))

print("region preview")
display(region_df.limit(20))

print("orders preview")
display(orders_df.limit(20))

# COMMAND ----------
# Create basic numeric profiling for key lineitem metric columns used in aggregation logic
lineitem_metrics_profile_df = lineitem_df.select(
    "l_quantity", "l_extendedprice", "l_discount", "l_tax"
).summary("count", "min", "max", "mean", "stddev")

display(lineitem_metrics_profile_df)

# COMMAND ----------
# Relationship check: lineitem.l_suppkey -> supplier.s_suppkey (join readiness and orphan keys)
li_supplier_join_check_df = (
    lineitem_df.alias("li")
    .join(supplier_df.alias("s"), F.col("li.l_suppkey") == F.col("s.s_suppkey"), "left")
    .agg(
        F.count("*").alias("lineitem_rows"),
        F.sum(F.col("s.s_suppkey").isNull().cast("int")).alias("orphan_lineitem_supplier_keys")
    )
    .limit(10)
)

display(li_supplier_join_check_df)

# COMMAND ----------
# Relationship check: supplier.s_nationkey -> nation.n_nationkey (join readiness and orphan keys)
supplier_nation_join_check_df = (
    supplier_df.alias("s")
    .join(nation_df.alias("n"), F.col("s.s_nationkey") == F.col("n.n_nationkey"), "left")
    .agg(
        F.count("*").alias("supplier_rows"),
        F.sum(F.col("n.n_nationkey").isNull().cast("int")).alias("orphan_supplier_nation_keys")
    )
)

display(supplier_nation_join_check_df)

# COMMAND ----------
# Relationship check: nation.n_regionkey -> region.r_regionkey (join readiness and orphan keys)
nation_region_join_check_df = (
    nation_df.alias("n")
    .join(region_df.alias("r"), F.col("n.n_regionkey") == F.col("r.r_regionkey"), "left")
    .agg(
        F.count("*").alias("nation_rows"),
        F.sum(F.col("r.r_regionkey").isNull().cast("int")).alias("orphan_nation_region_keys")
    )
)

display(nation_region_join_check_df)

# COMMAND ----------
# Relationship check: lineitem.l_orderkey -> orders.o_orderkey (join readiness and orphan keys)
li_orders_join_check_df = (
    lineitem_df.alias("li")
    .join(orders_df.alias("o"), F.col("li.l_orderkey") == F.col("o.o_orderkey"), "left")
    .agg(
        F.count("*").alias("lineitem_rows"),
        F.sum(F.col("o.o_orderkey").isNull().cast("int")).alias("orphan_lineitem_order_keys")
    )
)

display(li_orders_join_check_df)

# COMMAND ----------
# Build transformation-context sample at supplier/order/ship-date grain to validate business logic from example.sql
supplier_analytics_preview_df = (
    lineitem_df.alias("li")
    .join(supplier_df.alias("s"), F.col("li.l_suppkey") == F.col("s.s_suppkey"), "inner")
    .join(nation_df.alias("n"), F.col("s.s_nationkey") == F.col("n.n_nationkey"), "left")
    .join(region_df.alias("r"), F.col("n.n_regionkey") == F.col("r.r_regionkey"), "left")
    .join(orders_df.alias("o"), F.col("li.l_orderkey") == F.col("o.o_orderkey"), "left")
    .groupBy(
        F.col("li.l_suppkey").alias("supplier_key"),
        F.col("li.l_orderkey").alias("order_key"),
        F.col("li.l_shipdate").alias("ship_date_key"),
        F.col("s.s_nationkey").alias("nation_key"),
        F.col("n.n_regionkey").alias("region_key")
    )
    .agg(
        F.count("*").alias("total_line_items"),
        F.countDistinct("li.l_orderkey").alias("total_orders"),
        F.sum("li.l_quantity").alias("total_quantity"),
        F.round(F.sum("li.l_extendedprice"), 2).alias("gross_revenue"),
        F.round(F.sum(F.col("li.l_extendedprice") * (F.lit(1) - F.col("li.l_discount"))), 2).alias("net_revenue"),
        F.round(F.avg("li.l_discount"), 4).alias("average_discount"),
        F.round(F.avg("li.l_tax"), 4).alias("average_tax"),
        F.sum(F.when(F.col("li.l_receiptdate") <= F.col("li.l_commitdate"), F.lit(1)).otherwise(F.lit(0))).alias("on_time_shipments"),
        F.sum(F.when(F.col("li.l_receiptdate") > F.col("li.l_commitdate"), F.lit(1)).otherwise(F.lit(0))).alias("late_shipments"),
        F.round(F.avg(F.datediff(F.col("li.l_receiptdate"), F.col("li.l_commitdate"))), 2).alias("average_ship_delay_days"),
        F.max(F.datediff(F.col("li.l_receiptdate"), F.col("li.l_commitdate"))).alias("maximum_ship_delay_days")
    )
    .orderBy("supplier_key", "order_key", "ship_date_key")
)

display(supplier_analytics_preview_df.limit(100))

# COMMAND ----------
# Final exploration summary focused on source quality and join readiness before bronze/silver/gold design
exploration_summary_df = spark.createDataFrame(
    [
        ("samples.tpch.lineitem", "Fact-like source containing shipment, quantity, pricing, discount, tax, and date keys"),
        ("samples.tpch.supplier", "Supplier dimension source linked via l_suppkey -> s_suppkey"),
        ("samples.tpch.nation", "Nation dimension source linked via s_nationkey -> n_nationkey"),
        ("samples.tpch.region", "Region dimension source linked via n_regionkey -> r_regionkey"),
        ("samples.tpch.orders", "Order header source linked via l_orderkey -> o_orderkey"),
        ("Join readiness focus", "Validated orphan checks for all required relationships from transformation SQL"),
        ("Target grain", "supplier_key + order_key + ship_date_key with nation_key and region_key"),
        ("Gold metric context", "Revenue, discount, tax, shipment timeliness, and ship delay KPIs")
    ],
    ["topic", "notes"]
)

display(exploration_summary_df)
