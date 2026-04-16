# Databricks notebook source
# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
@dlt.table(name="supplier_performance_gold_mv")
@dlt.expect("valid_supplier_key", "supplier_key IS NOT NULL AND supplier_key > 0")
@dlt.expect("valid_order_key", "order_key IS NOT NULL AND order_key > 0")
@dlt.expect("valid_ship_date", "ship_date IS NOT NULL")
@dlt.expect("valid_quantity_non_negative", "total_quantity >= 0")
@dlt.expect("valid_revenue_non_negative", "gross_revenue >= 0 AND net_revenue >= 0")
@dlt.expect("valid_discount_range", "average_discount BETWEEN 0 AND 1")
@dlt.expect("valid_tax_range", "average_tax_rate BETWEEN 0 AND 1")
@dlt.expect("valid_delay_bounds", "maximum_shipping_delay_days >= average_shipping_delay_days")
def supplier_performance_gold_mv():
    lineitem_df = dlt.read("lineitem_order_validated_silver").alias("li")
    supplier_df = dlt.read("supplier_silver").alias("s")
    supplier_geo_df = dlt.read("supplier_geo_silver").alias("sg")

    return (
        lineitem_df
        .join(supplier_df, on="supplier_key", how="inner")
        .join(
            supplier_geo_df.select("supplier_key", "nation_key", "region_key"),
            on="supplier_key",
            how="left"
        )
        .groupBy("supplier_key", "order_key", "ship_date", "nation_key", "region_key")
        .agg(
            F.count(F.lit(1)).alias("total_line_items"),
            F.countDistinct("order_key").alias("total_orders"),
            F.sum("quantity").alias("total_quantity"),
            F.sum("gross_revenue").alias("gross_revenue"),
            F.sum("net_revenue").alias("net_revenue"),
            F.avg("discount").alias("average_discount"),
            F.avg("tax").alias("average_tax_rate"),
            F.sum(F.when(F.col("is_on_time_shipment") == 1, F.lit(1)).otherwise(F.lit(0))).alias("on_time_shipments"),
            F.sum(F.when(F.col("is_late_shipment") == 1, F.lit(1)).otherwise(F.lit(0))).alias("late_shipments"),
            F.avg("shipping_delay_days").alias("average_shipping_delay_days"),
            F.max("shipping_delay_days").alias("maximum_shipping_delay_days")
        )
    )