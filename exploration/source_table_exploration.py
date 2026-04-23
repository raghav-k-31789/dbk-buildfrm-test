# Databricks notebook source
# COMMAND ----------
# Brief markdown header for notebook purpose and scope
# MAGIC %md
# MAGIC # Source Table Exploration — Supplier Analytics DSS
# MAGIC Exploratory profiling of `samples.tpch` source tables to validate joins, data quality, and target-grain feasibility at **supplier + order + ship date**.  
# MAGIC Scope includes: `lineitem`, `orders`, `supplier`, `nation`, `region` only (per confirmed requirements).

# COMMAND ----------
# Import required PySpark functions and configure source table references
from pyspark.sql import functions as F

catalog_name = "samples"
schema_name = "tpch"

table_refs = {
    "lineitem": f"{catalog_name}.{schema_name}.lineitem",
    "orders": f"{catalog_name}.{schema_name}.orders",
    "supplier": f"{catalog_name}.{schema_name}.supplier",
    "nation": f"{catalog_name}.{schema_name}.nation",
    "region": f"{catalog_name}.{schema_name}.region",
}

# Load source tables into DataFrames for exploration
lineitem_df = spark.table(table_refs["lineitem"])
orders_df = spark.table(table_refs["orders"])
supplier_df = spark.table(table_refs["supplier"])
nation_df = spark.table(table_refs["nation"])
region_df = spark.table(table_refs["region"])

# COMMAND ----------
# Inspect schemas for all confirmed source tables
lineitem_df.printSchema()
orders_df.printSchema()
supplier_df.printSchema()
nation_df.printSchema()
region_df.printSchema()

# COMMAND ----------
# Compute row counts for each source table
row_counts_df = spark.createDataFrame(
    [
        ("lineitem", lineitem_df.count()),
        ("orders", orders_df.count()),
        ("supplier", supplier_df.count()),
        ("nation", nation_df.count()),
        ("region", region_df.count()),
    ],
    ["table_name", "row_count"]
)

display(row_counts_df.orderBy("table_name"))

# COMMAND ----------
# Null checks and uniqueness checks for key columns used in joins
key_quality_lineitem = lineitem_df.agg(
    F.sum(F.when(F.col("l_orderkey").isNull(), 1).otherwise(0)).alias("null_l_orderkey"),
    F.sum(F.when(F.col("l_suppkey").isNull(), 1).otherwise(0)).alias("null_l_suppkey"),
    F.sum(F.when(F.col("l_shipdate").isNull(), 1).otherwise(0)).alias("null_l_shipdate"),
    F.countDistinct("l_orderkey", "l_suppkey", "l_shipdate").alias("distinct_target_grain_keys")
)

key_quality_orders = orders_df.agg(
    F.sum(F.when(F.col("o_orderkey").isNull(), 1).otherwise(0)).alias("null_o_orderkey"),
    F.countDistinct("o_orderkey").alias("distinct_o_orderkey"),
    F.count("*").alias("orders_row_count")
)

key_quality_supplier = supplier_df.agg(
    F.sum(F.when(F.col("s_suppkey").isNull(), 1).otherwise(0)).alias("null_s_suppkey"),
    F.sum(F.when(F.col("s_nationkey").isNull(), 1).otherwise(0)).alias("null_s_nationkey"),
    F.countDistinct("s_suppkey").alias("distinct_s_suppkey"),
    F.count("*").alias("supplier_row_count")
)

key_quality_nation = nation_df.agg(
    F.sum(F.when(F.col("n_nationkey").isNull(), 1).otherwise(0)).alias("null_n_nationkey"),
    F.sum(F.when(F.col("n_regionkey").isNull(), 1).otherwise(0)).alias("null_n_regionkey"),
    F.countDistinct("n_nationkey").alias("distinct_n_nationkey"),
    F.count("*").alias("nation_row_count")
)

key_quality_region = region_df.agg(
    F.sum(F.when(F.col("r_regionkey").isNull(), 1).otherwise(0)).alias("null_r_regionkey"),
    F.countDistinct("r_regionkey").alias("distinct_r_regionkey"),
    F.count("*").alias("region_row_count")
)

display(key_quality_lineitem)
display(key_quality_orders)
display(key_quality_supplier)
display(key_quality_nation)
display(key_quality_region)

# COMMAND ----------
# Date range checks for shipment and commitment/receipt timelines
date_range_df = lineitem_df.agg(
    F.min("l_shipdate").alias("min_shipdate"),
    F.max("l_shipdate").alias("max_shipdate"),
    F.min("l_commitdate").alias("min_commitdate"),
    F.max("l_commitdate").alias("max_commitdate"),
    F.min("l_receiptdate").alias("min_receiptdate"),
    F.max("l_receiptdate").alias("max_receiptdate")
)

display(date_range_df)

# COMMAND ----------
# Basic descriptive stats for core numeric fields related to quantity, revenue, discount, and tax
lineitem_stats_df = lineitem_df.select(
    "l_quantity", "l_extendedprice", "l_discount", "l_tax"
).summary("count", "min", "max", "mean", "stddev")

display(lineitem_stats_df)

# COMMAND ----------
# Relationship validation: lineitem -> orders (orderkey)
li_to_orders_validation = lineitem_df.alias("li").join(
    orders_df.alias("o"),
    F.col("li.l_orderkey") == F.col("o.o_orderkey"),
    "left"
).agg(
    F.count("*").alias("lineitem_rows"),
    F.sum(F.when(F.col("o.o_orderkey").isNull(), 1).otherwise(0)).alias("unmatched_order_rows"),
    (F.sum(F.when(F.col("o.o_orderkey").isNull(), 1).otherwise(0)) / F.count("*")).alias("unmatched_order_ratio")
)

display(li_to_orders_validation)

# COMMAND ----------
# Relationship validation: lineitem -> supplier (suppkey)
li_to_supplier_validation = lineitem_df.alias("li").join(
    supplier_df.alias("s"),
    F.col("li.l_suppkey") == F.col("s.s_suppkey"),
    "left"
).agg(
    F.count("*").alias("lineitem_rows"),
    F.sum(F.when(F.col("s.s_suppkey").isNull(), 1).otherwise(0)).alias("unmatched_supplier_rows"),
    (F.sum(F.when(F.col("s.s_suppkey").isNull(), 1).otherwise(0)) / F.count("*")).alias("unmatched_supplier_ratio")
)

display(li_to_supplier_validation)

# COMMAND ----------
# Relationship validation: supplier -> nation -> region geographic path completeness
supplier_geo_validation = supplier_df.alias("s").join(
    nation_df.alias("n"),
    F.col("s.s_nationkey") == F.col("n.n_nationkey"),
    "left"
).join(
    region_df.alias("r"),
    F.col("n.n_regionkey") == F.col("r.r_regionkey"),
    "left"
).agg(
    F.count("*").alias("supplier_rows"),
    F.sum(F.when(F.col("n.n_nationkey").isNull(), 1).otherwise(0)).alias("unmatched_nation_rows"),
    F.sum(F.when(F.col("r.r_regionkey").isNull(), 1).otherwise(0)).alias("unmatched_region_rows")
)

display(supplier_geo_validation)

# COMMAND ----------
# Validate target grain feasibility at supplier + order + ship date by checking duplicates
target_grain_check_df = lineitem_df.groupBy("l_suppkey", "l_orderkey", "l_shipdate").agg(
    F.count("*").alias("line_items_in_grain")
)

target_grain_summary_df = target_grain_check_df.agg(
    F.count("*").alias("target_grain_row_count"),
    F.sum(F.when(F.col("line_items_in_grain") > 1, 1).otherwise(0)).alias("grain_rows_with_multiple_lineitems"),
    F.max("line_items_in_grain").alias("max_line_items_per_grain")
)

display(target_grain_summary_df)
display(target_grain_check_df.orderBy(F.desc("line_items_in_grain")).limit(50))

# COMMAND ----------
# Preview derivation of shipment and revenue metrics at required grain
supplier_performance_preview_df = lineitem_df.alias("li").join(
    supplier_df.alias("s"),
    F.col("li.l_suppkey") == F.col("s.s_suppkey"),
    "inner"
).join(
    nation_df.alias("n"),
    F.col("s.s_nationkey") == F.col("n.n_nationkey"),
    "inner"
).join(
    region_df.alias("r"),
    F.col("n.n_regionkey") == F.col("r.r_regionkey"),
    "inner"
).groupBy(
    F.col("li.l_suppkey").alias("supplier_key"),
    F.col("li.l_orderkey").alias("order_key"),
    F.col("li.l_shipdate").alias("ship_date"),
    F.col("n.n_nationkey").alias("nation_key"),
    F.col("r.r_regionkey").alias("region_key")
).agg(
    F.count("*").alias("total_line_items"),
    F.countDistinct("li.l_orderkey").alias("total_orders"),
    F.sum("li.l_quantity").alias("total_quantity"),
    F.sum("li.l_extendedprice").alias("gross_revenue"),
    F.sum(F.col("li.l_extendedprice") * (F.lit(1) - F.col("li.l_discount"))).alias("net_revenue"),
    F.avg("li.l_discount").alias("average_discount"),
    F.avg("li.l_tax").alias("average_tax"),
    F.sum(F.when(F.col("li.l_receiptdate") <= F.col("li.l_commitdate"), 1).otherwise(0)).alias("on_time_shipments"),
    F.sum(F.when(F.col("li.l_receiptdate") > F.col("li.l_commitdate"), 1).otherwise(0)).alias("late_shipments"),
    F.avg(F.datediff(F.col("li.l_receiptdate"), F.col("li.l_commitdate"))).alias("average_ship_delay_days"),
    F.max(F.datediff(F.col("li.l_receiptdate"), F.col("li.l_commitdate"))).alias("maximum_ship_delay_days")
)

display(supplier_performance_preview_df.limit(100))

# COMMAND ----------
# Final exploratory summary of key joins and explicit scope decision for ambiguity handling
# MAGIC %md
# MAGIC ## Exploration Summary
# MAGIC - Validated required joins:
# MAGIC   - `lineitem.l_orderkey = orders.o_orderkey`
# MAGIC   - `lineitem.l_suppkey = supplier.s_suppkey`
# MAGIC   - `supplier.s_nationkey = nation.n_nationkey`
# MAGIC   - `nation.n_regionkey = region.r_regionkey`
# MAGIC - Profiled row counts, schema, null/uniqueness of join keys, date ranges, and numeric stats.
# MAGIC - Confirmed target-grain feasibility at `supplier + order + ship_date` and previewed metric derivations.
# MAGIC - Ambiguity resolved per confirmed requirements: only the 5 source tables were explored; `part`, `partsupp`, and `customer` were intentionally excluded in this notebook.