/* 
--------------------------------------------------------------------------------
SQL Script: Amex Culinary Returns & Cancellations
--------------------------------------------------------------------------------
Objective:
    Identify Culinary class orders paid with American Express (Amex) cards 
    that were returned, canceled, or had their classes canceled, to support 
    financial reconciliation and operational reporting.

Definition:
    - Amex Culinary Orders:
        • Culinary orders placed through Payment instrument type = 'Amex'
    - Return or Cancellation:
        • Line item flagged as returned or canceled in OMS
        • Culinary class flagged as canceled in the course catalog

Scope:
    - Includes Culinary product types only.
    - Aggregated at the order-line level for detailed refund tracking.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH amex_culinary_orders AS (
    SELECT
        so.sfcc_order_number,
        oh.order_number AS oms_order_number,
        so.order_date,
        oh.source AS oms_source,
        so.customer_name,
        pi.card_type,
        pi.last_4,
        sl.line_id,
        sl.sku,
        oi.product_name,
        oi.product_type,
        cp.class_type,
        cp.start_date,
        cp.end_date,
        sl.is_return,
        sl.is_canceled,
        cp.is_class_cancelled,
        sl.quantity,
        sl.unit_price,
        sl.sub_total
    FROM sfcc.sfcc_order                AS so
    LEFT JOIN analytics.sales_header    AS oh
        ON oh.ecom_order_number = so.sfcc_order_number
    LEFT JOIN analytics.sales_line      AS sl
        ON sl.order_number = oh.order_number
    LEFT JOIN sfcc.sfcc_order_item      AS oi
        ON oi.sfcc_order_number = so.sfcc_order_number
       AND oi.sku = sl.sku
    LEFT JOIN sfcc.sfcc_payment_instrument AS pi
        ON pi.sfcc_order_number = so.sfcc_order_number
    LEFT JOIN analytics.culinary_products AS cp
        ON cp.sku = sl.sku
    WHERE pi.card_type = 'Amex'
      AND oi.product_type = 'Culinary'
      AND so.order_date >= DATE '2022-04-01'
      AND so.order_date <  DATE '2022-07-01'
),
line_level_agg AS (
    /* Aggregate at order-line level in case of duplicates from joins */
    SELECT
        sfcc_order_number,
        oms_order_number,
        order_date,
        oms_source,
        customer_name,
        card_type,
        last_4,
        line_id,
        sku,
        MIN(product_name) AS product_name,
        MIN(product_type) AS product_type,
        class_type,
        start_date,
        end_date,
        is_return,
        is_canceled,
        is_class_cancelled,
        SUM(quantity)  AS quantity,
        AVG(unit_price) AS unit_price,
        SUM(sub_total) AS line_revenue
    FROM amex_culinary_orders
    GROUP BY
        sfcc_order_number,
        oms_order_number,
        order_date,
        oms_source,
        customer_name,
        card_type,
        last_4,
        line_id,
        sku,
        class_type,
        start_date,
        end_date,
        is_return,
        is_canceled,
        is_class_cancelled
),
enriched_analytics AS (
    SELECT
        la.*,
        /* Total revenue per SFCC order (all lines) */
        SUM(la.line_revenue) OVER (
            PARTITION BY la.sfcc_order_number
        )  AS order_total_revenue,
        /* Total revenue per Amex card (by last 4 digits) */
        SUM(la.line_revenue) OVER (
            PARTITION BY la.last_4
        ) AS card_last4_total_revenue,
        /* Rank lines within an order by revenue contribution */
        RANK() OVER (
            PARTITION BY la.sfcc_order_number
            ORDER BY la.line_revenue DESC
        ) AS line_rank_in_order,
        /* Share of order revenue for each line */
        CASE 
            WHEN SUM(la.line_revenue) OVER (PARTITION BY la.sfcc_order_number) = 0
                THEN 0
            ELSE ROUND(
                100.0 * la.line_revenue
                / SUM(la.line_revenue) OVER (PARTITION BY la.sfcc_order_number),
                2
            )
        END  AS pct_of_order_revenue
    FROM line_level_agg AS la
)
SELECT
    sfcc_order_number AS sfcc_order_number,
    oms_order_number  AS oms_order_number,
    order_date        AS date_ordered,
    oms_source        AS source,
    customer_name,
    card_type,
    last_4            AS last_4_digits_cc,
    line_id,
    sku,
    product_name,
    product_type,
    class_type,
    start_date,
    end_date,
    is_return,
    is_canceled,
    is_class_cancelled,
    quantity,
    unit_price,
    line_revenue      AS sub_total,
    order_total_revenue,
    card_last4_total_revenue,
    line_rank_in_order,
    pct_of_order_revenue
FROM enriched_analytics
ORDER BY
    order_date,
    sfcc_order_number,
    line_rank_in_order;

