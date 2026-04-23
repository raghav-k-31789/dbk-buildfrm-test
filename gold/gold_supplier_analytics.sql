CREATE OR REFRESH MATERIALIZED VIEW dbk-test.gold.gold_supplier_analytics
AS
WITH silver_base AS (
  SELECT
    supplier_key,
    order_key,
    ship_date,
    nation_key,
    region_key,
    gross_revenue,
    net_revenue,
    discount_rate,
    tax_rate,
    on_time_flag,
    late_flag,
    ship_delay_days,
    line_item_count
  FROM dbk-test.silver.silver_supplier_analytics
),
aggregated AS (
  -- Aggregate to required grain: supplier + order + ship_date, with nation and region context
  SELECT
    supplier_key,
    order_key,
    ship_date,
    nation_key,
    region_key,
    SUM(line_item_count) AS total_line_items,
    COUNT(DISTINCT order_key) AS total_orders,
    SUM(quantity) AS total_quantity,
    SUM(gross_revenue) AS gross_revenue,
    SUM(net_revenue) AS net_revenue,
    AVG(discount_rate) AS average_discount,
    AVG(tax_rate) AS average_tax,
    SUM(on_time_flag) AS on_time_shipments,
    SUM(late_flag) AS late_shipments,
    AVG(ship_delay_days) AS average_ship_delay_days,
    MAX(ship_delay_days) AS maximum_ship_delay_days
  FROM dbk-test.silver.silver_supplier_analytics
  GROUP BY
    supplier_key,
    order_key,
    ship_date,
    nation_key,
    region_key
)
SELECT
  supplier_key,
  order_key,
  ship_date,
  nation_key,
  region_key,
  total_line_items,
  total_orders,
  total_quantity,
  gross_revenue,
  net_revenue,
  average_discount,
  average_tax,
  on_time_shipments,
  late_shipments,
  average_ship_delay_days,
  maximum_ship_delay_days
FROM aggregated;