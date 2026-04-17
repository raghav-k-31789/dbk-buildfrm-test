# Databricks notebook source

from pyspark.sql import functions as F

# Load required source tables from samples.tpch into DataFrames for exploration
source_tables = {
    "lineitem": "samples.tpch.lineitem",
    "supplier": "samples.tpch.supplier",
    "orders": "samples.tpch.orders",
    "nation": "samples.tpch.nation",
    "region": "samples.tpch.region"
}

dfs = {name: spark.table(table_name) for name, table_name in source_tables.items()}

# COMMAND ----------
# Capture row counts and schema metadata for each source table
row_count_rows = []
schema_rows = []

for name, df in dfs.items():
    row_count_rows.append((name, source_tables[name], df.count()))
    for field in df.schema.fields:
        schema_rows.append(
            (
                name,
                field.name,
                field.dataType.simpleString(),
                field.nullable
            )
        )

table_row_counts_df = spark.createDataFrame(row_count_rows, ["table_name", "full_table_name", "row_count"])
table_schema_df = spark.createDataFrame(schema_rows, ["table_name", "column_name", "data_type", "nullable"])

display(table_row_counts_df)
display(table_schema_df)

# COMMAND ----------
# Perform table-level completeness checks: all-null columns and non-null density by table/column
completeness_rows = []

for name, df in dfs.items():
    total_rows = df.count()
    agg_exprs = []
    for c in df.columns:
        agg_exprs.append(F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}__nulls"))
    null_counts_row = df.agg(*agg_exprs).collect()[0].asDict()

    for c in df.columns:
        null_count = int(null_counts_row[f"{c}__nulls"])
        non_null_count = total_rows - null_count
        completeness_rows.append(
            (
                name,
                c,
                total_rows,
                null_count,
                non_null_count,
                float(non_null_count / total_rows) if total_rows > 0 else None,
                null_count == total_rows
            )
        )

table_completeness_df = spark.createDataFrame(
    completeness_rows,
    ["table_name", "column_name", "total_rows", "null_count", "non_null_count", "non_null_ratio", "is_all_null_column"]
)

display(table_completeness_df.orderBy("table_name", "column_name"))

# COMMAND ----------
# Run column-level profiling for null counts and distinct counts across all source tables
column_profile_rows = []

for name, df in dfs.items():
    total_rows = df.count()
    agg_exprs = []
    for c in df.columns:
        agg_exprs.extend([
            F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"{c}__nulls"),
            F.countDistinct(F.col(c)).alias(f"{c}__distinct")
        ])

    prof_row = df.agg(*agg_exprs).collect()[0].asDict()
    for c in df.columns:
        null_count = int(prof_row[f"{c}__nulls"])
        distinct_count = int(prof_row[f"{c}__distinct"])
        column_profile_rows.append(
            (
                name,
                c,
                total_rows,
                null_count,
                distinct_count,
                float(distinct_count / total_rows) if total_rows > 0 else None
            )
        )

column_profile_df = spark.createDataFrame(
    column_profile_rows,
    ["table_name", "column_name", "total_rows", "null_count", "distinct_count", "distinct_ratio"]
)

display(column_profile_df.orderBy("table_name", "column_name"))

# COMMAND ----------
# Check duplicate patterns on business keys and expected primary keys
duplicate_checks = [
    ("supplier", ["s_suppkey"]),
    ("orders", ["o_orderkey"]),
    ("nation", ["n_nationkey"]),
    ("region", ["r_regionkey"]),
    ("lineitem", ["l_orderkey", "l_linenumber"])
]

dup_summary_rows = []
dup_samples = {}

for table_name, key_cols in duplicate_checks:
    df = dfs[table_name]
    grp = df.groupBy(*[F.col(c) for c in key_cols]).count().filter(F.col("count") > 1)
    duplicate_groups = grp.count()
    duplicate_rows = grp.agg(F.sum("count").alias("dup_rows")).collect()[0]["dup_rows"] if duplicate_groups > 0 else 0

    dup_summary_rows.append((
        table_name,
        ",".join(key_cols),
        duplicate_groups,
        int(duplicate_rows) if duplicate_rows is not None else 0
    ))
    dup_samples[table_name] = grp.orderBy(F.desc("count"))

duplicate_summary_df = spark.createDataFrame(
    dup_summary_rows,
    ["table_name", "key_columns", "duplicate_group_count", "duplicate_row_count"]
)

display(duplicate_summary_df.orderBy("table_name"))
for t in dup_samples:
    display(dup_samples[t])

# COMMAND ----------
# Profile numeric and date min/max values for relevant columns, and string length checks for dimensional text fields
numeric_date_specs = {
    "lineitem": {
        "numeric": ["l_quantity", "l_extendedprice", "l_discount", "l_tax"],
        "date": ["l_shipdate", "l_commitdate", "l_receiptdate"]
    },
    "orders": {
        "numeric": ["o_totalprice"],
        "date": ["o_orderdate"]
    }
}

string_length_specs = {
    "supplier": ["s_name", "s_address", "s_phone", "s_comment"],
    "nation": ["n_name", "n_comment"],
    "region": ["r_name", "r_comment"],
    "orders": ["o_orderstatus", "o_orderpriority", "o_clerk", "o_comment"],
    "lineitem": ["l_returnflag", "l_linestatus", "l_shipinstruct", "l_shipmode", "l_comment"]
}

minmax_rows = []
for table_name, specs in numeric_date_specs.items():
    df = dfs[table_name]
    for c in specs.get("numeric", []):
        vals = df.select(F.min(c).alias("min_val"), F.max(c).alias("max_val")).collect()[0]
        minmax_rows.append((table_name, c, "numeric", str(vals["min_val"]), str(vals["max_val"])))
    for c in specs.get("date", []):
        vals = df.select(F.min(c).alias("min_val"), F.max(c).alias("max_val")).collect()[0]
        minmax_rows.append((table_name, c, "date", str(vals["min_val"]), str(vals["max_val"])))

string_len_rows = []
for table_name, cols in string_length_specs.items():
    df = dfs[table_name]
    for c in cols:
        vals = df.select(
            F.min(F.length(F.col(c))).alias("min_len"),
            F.max(F.length(F.col(c))).alias("max_len"),
            F.avg(F.length(F.col(c))).alias("avg_len")
        ).collect()[0]
        string_len_rows.append((
            table_name,
            c,
            int(vals["min_len"]) if vals["min_len"] is not None else None,
            int(vals["max_len"]) if vals["max_len"] is not None else None,
            float(vals["avg_len"]) if vals["avg_len"] is not None else None
        ))

numeric_date_minmax_df = spark.createDataFrame(minmax_rows, ["table_name", "column_name", "column_type", "min_value", "max_value"])
string_length_profile_df = spark.createDataFrame(string_len_rows, ["table_name", "column_name", "min_length", "max_length", "avg_length"])

display(numeric_date_minmax_df.orderBy("table_name", "column_name"))
display(string_length_profile_df.orderBy("table_name", "column_name"))

# COMMAND ----------
# Validate join readiness across required table relationships and flag orphan keys
li = dfs["lineitem"]
sup = dfs["supplier"]
ordr = dfs["orders"]
nat = dfs["nation"]
reg = dfs["region"]

join_checks = []

# supplier -> nation
supplier_nation_orphans = sup.alias("s").join(nat.alias("n"), F.col("s.s_nationkey") == F.col("n.n_nationkey"), "left_anti")
join_checks.append(("supplier_to_nation", supplier_nation_orphans.count()))

# nation -> region
nation_region_orphans = nat.alias("n").join(reg.alias("r"), F.col("n.n_regionkey") == F.col("r.r_regionkey"), "left_anti")
join_checks.append(("nation_to_region", nation_region_orphans.count()))

# lineitem -> supplier
lineitem_supplier_orphans = li.alias("l").join(sup.alias("s"), F.col("l.l_suppkey") == F.col("s.s_suppkey"), "left_anti")
join_checks.append(("lineitem_to_supplier", lineitem_supplier_orphans.count()))

# lineitem -> orders
lineitem_orders_orphans = li.alias("l").join(ordr.alias("o"), F.col("l.l_orderkey") == F.col("o.o_orderkey"), "left_anti")
join_checks.append(("lineitem_to_orders", lineitem_orders_orphans.count()))

join_readiness_df = spark.createDataFrame(join_checks, ["relationship", "orphan_row_count"])
display(join_readiness_df)

display(supplier_nation_orphans.limit(50))
display(nation_region_orphans.limit(50))
display(lineitem_supplier_orphans.limit(50))
display(lineitem_orders_orphans.limit(50))

# COMMAND ----------
# Execute business-rule quality checks for supplier analytics metrics and produce failed-rule samples
business_rule_checks = {}

# Non-negative quantities and prices
business_rule_checks["negative_quantity"] = li.filter(F.col("l_quantity") < 0)
business_rule_checks["negative_extendedprice"] = li.filter(F.col("l_extendedprice") < 0)

# Discount and tax range checks [0,1]
business_rule_checks["discount_out_of_range"] = li.filter((F.col("l_discount") < 0) | (F.col("l_discount") > 1))
business_rule_checks["tax_out_of_range"] = li.filter((F.col("l_tax") < 0) | (F.col("l_tax") > 1))

# Date ordering checks
business_rule_checks["shipdate_after_receiptdate"] = li.filter(F.col("l_shipdate") > F.col("l_receiptdate"))
business_rule_checks["commitdate_after_receiptdate"] = li.filter(F.col("l_commitdate") > F.col("l_receiptdate"))
business_rule_checks["shipdate_after_commitdate"] = li.filter(F.col("l_shipdate") > F.col("l_commitdate"))

# Delay calculation sanity: negative delay indicates early/on-time receipt; keep for exploration
lineitem_with_delay = li.withColumn("ship_delay_days", F.datediff(F.col("l_receiptdate"), F.col("l_commitdate")))
business_rule_checks["delay_gt_365_days"] = lineitem_with_delay.filter(F.col("ship_delay_days") > 365)
business_rule_checks["delay_lt_minus365_days"] = lineitem_with_delay.filter(F.col("ship_delay_days") < -365)

business_rule_summary_rows = []
for rule_name, rdf in business_rule_checks.items():
    business_rule_summary_rows.append((rule_name, rdf.count()))

business_rule_summary_df = spark.createDataFrame(business_rule_summary_rows, ["rule_name", "failed_row_count"])
display(business_rule_summary_df.orderBy("rule_name"))

for rule_name, rdf in business_rule_checks.items():
    display(rdf.limit(50))

# COMMAND ----------
# Explore final target grain uniqueness: supplier + order + ship_date duplicate pattern analysis
grain_cols = ["l_suppkey", "l_orderkey", "l_shipdate"]

grain_dup_groups_df = (
    li.groupBy(*grain_cols)
      .count()
      .filter(F.col("count") > 1)
      .orderBy(F.desc("count"))
)

grain_duplicate_summary_df = grain_dup_groups_df.agg(
    F.count("*").alias("duplicate_group_count"),
    F.sum("count").alias("duplicate_row_count"),
    F.max("count").alias("max_rows_per_group")
)

display(grain_duplicate_summary_df)
display(grain_dup_groups_df.limit(200))

# COMMAND ----------
# Create consolidated quality scorecard and final exploratory outputs for notebook review
row_count_lookup = {r["table_name"]: r["row_count"] for r in table_row_counts_df.collect()}
orphan_lookup = {r["relationship"]: r["orphan_row_count"] for r in join_readiness_df.collect()}
dup_lookup = {r["table_name"]: r["duplicate_group_count"] for r in duplicate_summary_df.collect()}

scorecard_rows = []
for t in source_tables.keys():
    completeness_issues = table_completeness_df.filter((F.col("table_name") == t) & (F.col("is_all_null_column") == True)).count()
    duplicates = int(dup_lookup.get(t, 0))
    scorecard_rows.append((
        t,
        int(row_count_lookup.get(t, 0)),
        int(completeness_issues),
        duplicates
    ))

quality_scorecard_df = spark.createDataFrame(
    scorecard_rows,
    ["table_name", "row_count", "all_null_column_count", "duplicate_group_count_on_key_checks"]
)

display(quality_scorecard_df.orderBy("table_name"))
display(join_readiness_df.orderBy("relationship"))
display(business_rule_summary_df.orderBy(F.desc("failed_row_count"), "rule_name"))
