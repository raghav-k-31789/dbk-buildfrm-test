# Databricks notebook source
# COMMAND ----------
import dlt
from pyspark.sql import functions as F

# COMMAND ----------
@dlt.table(
    name="supplier_performance_gold_mv",
    comment="Gold materialized view at grain supplier_key, order_key, ship_date with supplier performance metrics."
)
@dlt.expect_or_drop("valid_supplier_key", "supplier_key IS NOT NULL AND supplier_key > 0")
@dlt.expect_or_drop("valid_order_key", "order_key IS NOT NULL AND order_key > 0")
@dlt.expect_or_drop("valid_ship_date", "ship_date IS NOT NULL")
@dlt.expect("valid_quantity_non_negative", "total_quantity >= 0")
@dlt.expect("valid_revenue_non_negative", "gross_revenue >= 0 AND net_revenue >= 0")
@dlt.expect("valid_discount_range", "average_discount BETWEEN 0 AND 1")
@dlt.expect("valid_tax_range", "average_tax_rate BETWEEN 0 AND 1")
@dlt.expect("valid_delay_bounds", "maximum_shipping_delay_days >= average_shipping_delay_days")
def supplier_performance_gold_mv():
    lineitem = dlt.read("workspace.silver.lineitem_silver_mv").alias("l")
    orders = dlt.read("workspace.silver.orders_silver_mv").alias("o")
    supplier = dlt.read("workspace.silver.supplier_silver_mv").alias("s")
    nation_region = dlt.read("workspace.silver.supplier_nation_region_silver_mv").alias("snr")

    joined = (
        lineitem.join(orders, F.col("l.order_key") == F.col("o.order_key"), "inner")
                .join(supplier, F.col("l.supplier_key") == F.col("s.supplier_key"), "inner")
                .join(
                    nation_region,
                    (F.col("l.supplier_key") == F.col("snr.supplier_key")) &
                    (F.col("s.nation_key") == F.col("snr.nation_key")),
                    "left"
                )
    )

    return (
        joined.groupBy(
            F.col("l.supplier_key").alias("supplier_key"),
            F.col("l.order_key").alias("order_key"),
            F.col("l.ship_date").alias("ship_date"),
            F.col("snr.nation_key").alias("nation_key"),
            F.col("snr.region_key").alias("region_key")
        )
        .agg(
            F.count(F.lit(1)).cast("long").alias("total_line_items"),
            F.countDistinct(F.col("l.order_key")).cast("long").alias("total_orders"),
            F.sum(F.col("l.quantity")).cast("decimal(18,2)").alias("total_quantity"),
            F.sum(F.col("l.extended_price")).cast("decimal(18,2)").alias("gross_revenue"),
            F.sum(F.col("l.net_revenue")).cast("decimal(18,2)").alias("net_revenue"),
            F.avg(F.col("l.discount")).cast("decimal(10,6)").alias("average_discount"),
            F.avg(F.col("l.tax")).cast("decimal(10,6)").alias("average_tax_rate"),
            F.sum(F.when(F.col("l.is_on_time_shipment") == 1, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("on_time_shipments"),
            F.sum(F.when(F.col("l.is_late_shipment") == 1, F.lit(1)).otherwise(F.lit(0))).cast("long").alias("late_shipments"),
            F.avg(F.col("l.shipping_delay_days")).cast("decimal(10,4)").alias("average_shipping_delay_days"),
            F.max(F.col("l.shipping_delay_days")).cast("int").alias("maximum_shipping_delay_days")
        )
    )

# COMMAND ----------
display(dlt.read("supplier_performance_gold_mv"))