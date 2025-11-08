/* 
--------------------------------------------------------------------------------
SQL Script: Hourly Demand Analysis
--------------------------------------------------------------------------------
Objective:
    Measure hourly merchandise demand across Ecommerce and BOPIS (Buy Online, 
    Pick Up In Store) orders.

Definition:
    - Demand:
        • Total merchandise value of completed (non-canceled, non-returned) 
          order lines.
    - Order Type:
        • Includes Ecommerce and BOPIS transactions only.
        • Excludes Culinary product types and non-OROMS sources.
    - Time Granularity:
        • Aggregated by order date and hour of day.

Scope:
    - Filters out returns, cancellations, and non-merchandise adjustments.

Author: Cesar Rodriguez
--------------------------------------------------------------------------------
*/

WITH filtered_sales AS (
    SELECT
        sl.order_number,
        sl.date_ordered,
        sl.merchandise,
        sl.order_line_type,
        sh.source,
        oi.product_type
    FROM analytics.sales_line        AS sl
    JOIN analytics.sales_header      AS sh
        ON sh.order_number = sl.order_number
    LEFT JOIN sfcc.sfcc_order        AS so
        ON so.sfcc_order_number = sh.ecom_order_number
    LEFT JOIN sfcc.sfcc_order_item   AS oi
        ON oi.sfcc_order_number = so.sfcc_order_number
       AND oi.sku = sl.sku
    WHERE sl.is_canceled = 'FALSE'
      AND sl.is_return  = 'FALSE'
      AND sl.order_line_type IN ('ecommerce', 'bopis')
      AND sh."source" = 'oroms'
      AND oi.product_type <> 'Culinary'
      AND CAST(sl.date_ordered AS DATE) BETWEEN DATE '2022-10-02' AND DATE '2022-10-14'
),
hourly_demand AS (
    SELECT
        CAST(date_ordered AS DATE) AS order_date,
        EXTRACT(HOUR FROM date_ordered) AS order_hour,
        SUM(merchandise) AS demand
    FROM filtered_sales
    GROUP BY
        CAST(date_ordered AS DATE),
        EXTRACT(HOUR FROM date_ordered)
),
hourly_enriched AS (
    SELECT
        order_date,
        order_hour,
        demand,
        SUM(demand) OVER (PARTITION BY order_date)   AS daily_demand,
        ROUND(
            100.0 * demand
            / NULLIF(SUM(demand) OVER (PARTITION BY order_date), 0),
            2
        ) AS pct_of_daily_demand,
        SUM(demand) OVER (
            PARTITION BY order_date
            ORDER BY order_hour
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_demand
    FROM hourly_demand
)

SELECT
    order_date      AS "Date",
    order_hour      AS "Hour",
    demand          AS "Demand",
    daily_demand    AS "Daily_Demand",
    pct_of_daily_demand AS "Pct_of_Daily_Demand",
    cumulative_demand   AS "Cumulative_Demand"
FROM hourly_enriched
ORDER BY
    order_date,
    order_hour;
