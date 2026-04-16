# Databricks notebook source
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------
catalog = "samples"
schema = "tpch"

table_names = ["lineitem", "orders", "supplier", "nation", "region"]
tables = {t: spark.table(f"{catalog}.{schema}.{t}") for t in table_names}

# COMMAND ----------
profile_rows = []
for t, df in tables.items():
    profile_rows.append((t, df.count(), len(df.columns)))
profile_df = spark.createDataFrame(profile_rows, ["table_name", "row_count", "column_count"]).orderBy("table_name")
display(profile_df)

# COMMAND ----------
for t, df in tables.items():
    print(f"Schema for {catalog}.{schema}.{t}")
    df.printSchema()

# COMMAND ----------
key_columns = {
    "lineitem": ["l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_shipdate", "l_commitdate", "l_receiptdate"],
    "orders": ["o_orderkey", "o_custkey", "o_orderstatus", "o_orderdate"],
    "supplier": ["s_suppkey", "s_nationkey", "s_name"],
    "nation": ["n_nationkey", "n_regionkey", "n_name"],
    "region": ["r_regionkey", "r_name"]
}

# COMMAND ----------
null_blank_results = []
for t, cols in key_columns.items():
    df = tables[t]
    total = df.count()
    for c in cols:
        is_blank_expr = F.when(F.col(c).cast("string").isNull(), F.lit(True)).otherwise(F.trim(F.col(c).cast("string")) == "")
        cnt = df.filter(is_blank_expr).count()
        pct = (cnt / total * 100.0) if total > 0 else 0.0
        null_blank_results.append((t, c, cnt, pct))
null_blank_df = spark.createDataFrame(
    null_blank_results,
    ["table_name", "column_name", "null_or_blank_count", "null_or_blank_pct"]
).orderBy("table_name", "column_name")
display(null_blank_df)

# COMMAND ----------
duplicate_defs = {
    "lineitem": ["l_orderkey", "l_linenumber"],
    "orders": ["o_orderkey"],
    "supplier": ["s_suppkey"],
    "nation": ["n_nationkey"],
    "region": ["r_regionkey"]
}

# COMMAND ----------
dup_results = []
for t, keys in duplicate_defs.items():
    df = tables[t]
    total = df.count()
    dup_count = (
        df.groupBy(*keys)
          .count()
          .filter(F.col("count") > 1)
          .agg(F.sum("count").alias("dup_rows"))
          .collect()[0]["dup_rows"]
    )
    dup_count = int(dup_count) if dup_count is not None else 0
    dup_pct = (dup_count / total * 100.0) if total > 0 else 0.0
    dup_results.append((t, ",".join(keys), dup_count, dup_pct))
dup_summary_df = spark.createDataFrame(
    dup_results, ["table_name", "expected_key", "duplicate_row_count", "duplicate_row_pct"]
).orderBy("table_name")
display(dup_summary_df)

# COMMAND ----------
lineitem = tables["lineitem"]
orders = tables["orders"]
supplier = tables["supplier"]
nation = tables["nation"]
region = tables["region"]

# COMMAND ----------
li_orders_unmatched = (
    lineitem.alias("li")
    .join(orders.alias("o"), F.col("li.l_orderkey") == F.col("o.o_orderkey"), "left")
    .filter(F.col("o.o_orderkey").isNull())
)
li_supplier_unmatched = (
    lineitem.alias("li")
    .join(supplier.alias("s"), F.col("li.l_suppkey") == F.col("s.s_suppkey"), "left")
    .filter(F.col("s.s_suppkey").isNull())
)
supplier_nation_unmatched = (
    supplier.alias("s")
    .join(nation.alias("n"), F.col("s.s_nationkey") == F.col("n.n_nationkey"), "left")
    .filter(F.col("n.n_nationkey").isNull())
)
nation_region_unmatched = (
    nation.alias("n")
    .join(region.alias("r"), F.col("n.n_regionkey") == F.col("r.r_regionkey"), "left")
    .filter(F.col("r.r_regionkey").isNull())
)

join_summary = spark.createDataFrame([
    ("lineitem_to_orders", lineitem.count(), li_orders_unmatched.count()),
    ("lineitem_to_supplier", lineitem.count(), li_supplier_unmatched.count()),
    ("supplier_to_nation", supplier.count(), supplier_nation_unmatched.count()),
    ("nation_to_region", nation.count(), nation_region_unmatched.count())
], ["join_path", "base_rows", "unmatched_rows"]) \
.withColumn("unmatched_pct", F.when(F.col("base_rows") > 0, F.col("unmatched_rows") / F.col("base_rows") * 100.0).otherwise(F.lit(0.0)))

display(join_summary)

# COMMAND ----------
display(li_orders_unmatched.select("l_orderkey", "l_suppkey", "l_shipdate").limit(100))
display(li_supplier_unmatched.select("l_orderkey", "l_suppkey", "l_shipdate").limit(100))
display(supplier_nation_unmatched.select("s_suppkey", "s_nationkey").limit(100))
display(nation_region_unmatched.select("n_nationkey", "n_regionkey").limit(100))

# COMMAND ----------
lineitem_quality = (
    lineitem
    .withColumn("net_revenue", F.col("l_extendedprice") * (F.lit(1.0) - F.col("l_discount")))
    .withColumn("delay_days", F.datediff(F.col("l_receiptdate"), F.col("l_commitdate")))
    .withColumn("ship_vs_order_days", F.datediff(F.col("l_shipdate"), F.col("l_commitdate")))
)

# COMMAND ----------
numeric_checks = lineitem_quality.select(
    F.sum(F.when(F.col("l_quantity") <= 0, 1).otherwise(0)).alias("qty_le_zero"),
    F.sum(F.when(F.col("l_extendedprice") < 0, 1).otherwise(0)).alias("extendedprice_lt_zero"),
    F.sum(F.when((F.col("l_discount") < 0) | (F.col("l_discount") > 1), 1).otherwise(0)).alias("discount_out_of_range"),
    F.sum(F.when((F.col("l_tax") < 0) | (F.col("l_tax") > 1), 1).otherwise(0)).alias("tax_out_of_range"),
    F.sum(F.when(F.col("net_revenue") < 0, 1).otherwise(0)).alias("net_revenue_lt_zero")
)
display(numeric_checks)

# COMMAND ----------
date_checks = lineitem_quality.select(
    F.sum(F.when(F.col("l_shipdate") < F.col("l_commitdate"), 1).otherwise(0)).alias("ship_before_commit_count"),
    F.sum(F.when(F.col("l_receiptdate") < F.col("l_shipdate"), 1).otherwise(0)).alias("receipt_before_ship_count"),
    F.sum(F.when(F.col("delay_days") < 0, 1).otherwise(0)).alias("negative_delay_count"),
    F.avg(F.col("delay_days")).alias("avg_delay_days"),
    F.max(F.col("delay_days")).alias("max_delay_days"),
    F.min(F.col("delay_days")).alias("min_delay_days")
)
display(date_checks)

# COMMAND ----------
domain_checks = lineitem.select(
    F.count("*").alias("total_rows"),
    F.sum(F.when(~F.col("l_returnflag").isin("R", "A", "N"), 1).otherwise(0)).alias("invalid_returnflag_count"),
    F.sum(F.when(~F.col("l_linestatus").isin("O", "F"), 1).otherwise(0)).alias("invalid_linestatus_count")
)
display(domain_checks)

# COMMAND ----------
display(lineitem.groupBy("l_returnflag").count().orderBy("l_returnflag"))
display(lineitem.groupBy("l_linestatus").count().orderBy("l_linestatus"))
display(orders.groupBy("o_orderstatus").count().orderBy("o_orderstatus"))

# COMMAND ----------
outlier_summary = lineitem_quality.select(
    F.expr("percentile_approx(l_quantity, array(0.01,0.05,0.5,0.95,0.99))").alias("qty_percentiles"),
    F.expr("percentile_approx(l_extendedprice, array(0.01,0.05,0.5,0.95,0.99))").alias("price_percentiles"),
    F.expr("percentile_approx(l_discount, array(0.01,0.05,0.5,0.95,0.99))").alias("discount_percentiles"),
    F.expr("percentile_approx(l_tax, array(0.01,0.05,0.5,0.95,0.99))").alias("tax_percentiles"),
    F.expr("percentile_approx(delay_days, array(0.01,0.05,0.5,0.95,0.99))").alias("delay_percentiles")
)
display(outlier_summary)

# COMMAND ----------
quality_summary = (
    null_blank_df.select(
        F.lit("null_blank").alias("check_type"),
        F.concat_ws(".", F.col("table_name"), F.col("column_name")).alias("check_name"),
        F.col("null_or_blank_count").cast("long").alias("issue_count"),
        F.col("null_or_blank_pct").alias("issue_pct")
    )
    .unionByName(
        dup_summary_df.select(
            F.lit("duplicate").alias("check_type"),
            F.col("table_name").alias("check_name"),
            F.col("duplicate_row_count").cast("long").alias("issue_count"),
            F.col("duplicate_row_pct").alias("issue_pct")
        )
    )
    .unionByName(
        join_summary.select(
            F.lit("referential_integrity").alias("check_type"),
            F.col("join_path").alias("check_name"),
            F.col("unmatched_rows").cast("long").alias("issue_count"),
            F.col("unmatched_pct").alias("issue_pct")
        )
    )
)
display(quality_summary.orderBy("check_type", F.desc("issue_count")))

# COMMAND ----------
failed_numeric_samples = lineitem_quality.filter(
    (F.col("l_quantity") <= 0) |
    (F.col("l_extendedprice") < 0) |
    (F.col("l_discount") < 0) | (F.col("l_discount") > 1) |
    (F.col("l_tax") < 0) | (F.col("l_tax") > 1) |
    (F.col("net_revenue") < 0)
).select(
    "l_orderkey", "l_linenumber", "l_suppkey", "l_quantity", "l_extendedprice", "l_discount", "l_tax", "net_revenue"
).limit(100)

failed_date_samples = lineitem_quality.filter(
    (F.col("l_receiptdate") < F.col("l_shipdate")) |
    (F.col("delay_days") < 0)
).select(
    "l_orderkey", "l_linenumber", "l_suppkey", "l_shipdate", "l_commitdate", "l_receiptdate", "delay_days"
).limit(100)

display(failed_numeric_samples)
display(failed_date_samples)

# COMMAND ----------
display(quality_summary.orderBy(F.desc("issue_pct"), F.desc("issue_count")))