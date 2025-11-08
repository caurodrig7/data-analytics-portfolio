/* 
--------------------------------------------------------------------------------
SQL Script: Cooking Classes Filling Rate Report
--------------------------------------------------------------------------------
Objective:
    Measure seat utilization and revenue performance of Culinary classes 
    across store locations.

Definition:
    - Fill Rate:
        • Ratio of paid seats to total available seats for each class.
    - Available Seats:
        • Difference between maximum seats and paid seats.
    - Revenue Metrics:
        • Actual: Sum of paid sales.
        • Potential: Actual sales plus value of unfilled seats at base price.

Scope:
    - Aggregated by class session (location, start date, SKU, and instructor).

Author: Cesar Rodriguez
--------------------------------------------------------------------------------
*/
 
WITH filtered_classes AS (
    SELECT
        cp.location_code,
        cp.start_date,
        cp.sku,
        cp.instructor_name,
        cp.maximum_seats,
        cp.is_class_cancelled,
        cp.price,
        st.name               AS store_name,
        st.district_code,
        pr.short_description  AS class_name
    FROM analytics.culinary_products AS cp
    LEFT JOIN analytics.locations AS st
        ON st.location_code = cp.location_code
    LEFT JOIN analytics.products AS pr
        ON pr.sku = cp.sku
    WHERE cp.sku NOT LIKE 'CSA%'   -- exclude CSA subscriptions
      AND cp.start_date >= TIMESTAMP '2023-01-29 00:00:00'
      AND cp.start_date <= TIMESTAMP '2024-02-03 23:59:59'
),
sales_enriched AS (
    SELECT
        fc.location_code,
        fc.start_date,
        fc.sku,
        fc.instructor_name,
        fc.maximum_seats,
        fc.is_class_cancelled,
        fc.price,
        fc.store_name,
        fc.district_code,
        fc.class_name,
        sl.quantity,
        sl.sub_total,
        oi.unit_price
    FROM filtered_classes AS fc
    LEFT JOIN analytics.sales_line AS sl
        ON  sl.sku = fc.sku
        AND (sl.is_return  <> 'TRUE' OR sl.is_return  IS NULL)
        AND (sl.is_canceled <> 'TRUE' OR sl.is_canceled IS NULL)
    LEFT JOIN analytics.sales_header AS sh
        ON sh.order_number = sl.order_number
    LEFT JOIN sfcc.sfcc_order_item AS oi
        ON  oi.sfcc_order_number = sh.ecom_order_number
        AND oi.sku = sl.sku
),
class_aggregated AS (
    SELECT
        se.store_name,
        se.location_code                         AS store,
        se.district_code,
        CAST(se.start_date AS DATE)              AS date_start,
        CAST(se.start_date AS TIME)              AS time_start,
        se.sku                                   AS class_sku,
        se.class_name,
        se.instructor_name                       AS chef_name,
        se.maximum_seats                         AS max_seats,
        se.is_class_cancelled,
        SUM(ISNULL(se.quantity, 0))              AS paid_in_full,
        COALESCE(
            AVG(se.unit_price),
            MIN(se.price)
        )                                        AS cost,
        SUM(ISNULL(se.sub_total, 0))             AS actual
    FROM sales_enriched AS se
    GROUP BY
        se.store_name,
        se.location_code,
        se.district_code,
        se.start_date,
        se.sku,
        se.class_name,
        se.instructor_name,
        se.maximum_seats,
        se.is_class_cancelled
),
class_metrics AS (
    SELECT
        ca.store_name,
        ca.store,
        ca.district_code,
        ca.date_start,
        ca.time_start,
        ca.class_sku,
        ca.class_name,
        ca.chef_name,
        ca.max_seats,
        ca.is_class_cancelled,
        ca.paid_in_full,
        (ca.max_seats - ca.paid_in_full)                 AS available_seats,
        CASE 
            WHEN ca.max_seats = 0 THEN 0
            ELSE CAST(1.0 * ca.paid_in_full / ca.max_seats AS DECIMAL(10,4))
        END                                              AS fill_rate,
        ca.cost,
        ca.actual,
        ca.actual + (ca.max_seats - ca.paid_in_full) * ca.cost
                                                         AS potential,
        -- District-level benchmarking
        SUM(ca.actual) OVER (
            PARTITION BY ca.district_code
        )                                                AS district_total_revenue,
        AVG(
            CASE 
                WHEN ca.max_seats = 0 THEN 0
                ELSE CAST(1.0 * ca.paid_in_full / ca.max_seats AS DECIMAL(10,4))
            END
        ) OVER (
            PARTITION BY ca.district_code
        )                                                AS district_avg_fill_rate,
        RANK() OVER (
            PARTITION BY ca.district_code
            ORDER BY 
                CASE 
                    WHEN ca.max_seats = 0 THEN 0
                    ELSE CAST(1.0 * ca.paid_in_full / ca.max_seats AS DECIMAL(10,4))
                END DESC
        )                                                AS fill_rate_rank_in_district
    FROM class_aggregated AS ca
)
SELECT
    store_name,
    store,
    district_code,
    date_start,
    time_start,
    class_sku,
    class_name,
    chef_name,
    max_seats,
    paid_in_full,
    available_seats,
    fill_rate,
    cost,
    potential,
    actual,
    is_class_cancelled,
    district_total_revenue,
    district_avg_fill_rate,
    fill_rate_rank_in_district
FROM class_metrics
ORDER BY
    date_start,
    store;
