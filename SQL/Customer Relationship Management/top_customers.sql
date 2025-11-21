/* 
--------------------------------------------------------------------------------
SQL Script: Top 5000 Customers – Multi-Year Qualification & Ranking
--------------------------------------------------------------------------------
Objective:
    Identify high-value customers by applying:
        • Multi-year spend and order-count thresholds (2022–2025)
        • Email validity rules and exclusion lists
        • Ranking based on total sales performance

    Results feed CRM programs targeting high-value customers for:
        • Loyalty initiatives
        • Win-back campaigns
        • Personalized high-value segmentation

Definition:
    - Qualification Years (2022–2025):
        • Customers must meet minimum spend AND maximum order count requirements
          in each corresponding year.
        • Year-specific rules:
              – 2025: ≥ $400 sales and ≤ 50 orders
              – 2024: ≥ $300 sales and ≤ 50 orders
              – 2023: ≥ $200 sales and ≤ 50 orders
              – 2022: Between 1 and 50 orders

    - Valid Orders:
        • purchase_order = 'Y'
        • target_key <> -1
        • net_amount > 0
        • Used to calculate yearly sales, order counts, and final totals.

    - Customer Email Rules:
        • Remove internal emails:
              – ‘surlatable’, ‘investcorp’, ‘marketplace.amazon’
        • Enforce:
              – Non-null email
              – Valid syntax (email_valid_syntax=1)
              – Customer not marked Do-Not-Email
        • Exclude specific target_key values provided by business.

    - Customer Metrics:
        • total_orders: distinct orders across all years
        • total_sales: total net sales across all valid orders
        • sales_rank and dense_rank: order by descending revenue
        • sales_share: customer share of total revenue
        • cum_sales_share: cumulative share of revenue (Pareto curve)

Scope:
    - Customers active in any of the 2012–2015 qualification years.
    - Output includes only customers meeting *all* yearly rules.
    - Final result limited to Top 5000 based on descending total_sales.

Processing Steps:
    1. Build base_orders for qualification years.
    2. Aggregate yearly metrics (orders and sales) per customer.
    3. Determine if each customer satisfies the rule for each year.
    4. Keep customers who meet *every* year’s condition (logical AND).
    5. Pull all valid orders for qualifying customers.
    6. Aggregate total orders and sales at the customer level.
    7. Join email + customer profile; apply email and exclusion filters.
    8. Use window functions to compute:
         • row_number, rank, dense_rank
         • sales_share, cumulative revenue share
    9. Select Top 5000 customers by revenue.
   10. Output enriched customer profile + ranking + sales KPIs.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH
-- 1) Base orders for qualification years
base_orders AS (
    SELECT
        oh.target_key,
        oh.order_number,
        oh.order_year,
        oh.net_amount
    FROM ims.order_header oh
    WHERE oh.net_amount    > 0
      AND oh.purchase_order = 'Y'
      AND oh.target_key    <> -1
      AND oh.order_year BETWEEN 2022 AND 2025
),

-- 2) Yearly per-customer metrics (orders & sales)
yearly_metrics AS (
    SELECT
        target_key,
        order_year,
        COUNT(DISTINCT order_number) AS orders_per_year,
        SUM(net_amount)             AS sales_per_year
    FROM base_orders
    GROUP BY target_key, order_year
),

-- 3) One row per customer with flags for each year’s qualification logic
qualified_by_year AS (
    SELECT
        ym.target_key,
        MAX(
            CASE
                WHEN order_year = 2025
                     AND sales_per_year >= 400
                     AND orders_per_year <= 50
                THEN 1 ELSE 0
            END
        ) AS ok_2025,
        MAX(
            CASE
                WHEN order_year = 2024
                     AND sales_per_year >= 300
                     AND orders_per_year <= 50
                THEN 1 ELSE 0
            END
        ) AS ok_2024,
        MAX(
            CASE
                WHEN order_year = 2023
                     AND sales_per_year >= 200
                     AND orders_per_year <= 50
                THEN 1 ELSE 0
            END
        ) AS ok_2023,
        MAX(
            CASE
                WHEN order_year = 2022
                     AND orders_per_year BETWEEN 1 AND 50
                THEN 1 ELSE 0
            END
        ) AS ok_2022
    FROM yearly_metrics ym
    GROUP BY ym.target_key
),

-- 4) Customers that meet ALL year-specific rules (logical intersection)
eligible_customers AS (
    SELECT
        q.target_key
    FROM qualified_by_year q
    WHERE q.ok_2025 = 1
      AND q.ok_2024 = 1
      AND q.ok_2023 = 1
      AND q.ok_2022 = 1
),

-- 5) All qualifying orders (any year) for eligible customers
eligible_orders AS (
    SELECT
        oh.target_key,
        oh.order_number,
        oh.net_amount
    FROM ims.order_header oh
    JOIN eligible_customers ec
      ON oh.target_key = ec.target_key
    WHERE oh.net_amount    > 0
      AND oh.purchase_order = 'Y'
      AND oh.target_key    <> -1
),

-- 6) Customer-level sales & order counts
customer_sales AS (
    SELECT
        eo.target_key,
        COUNT(DISTINCT eo.order_number) AS total_orders,
        SUM(eo.net_amount)              AS total_sales
    FROM eligible_orders eo
    GROUP BY eo.target_key
),

-- 7) Apply email & contact-quality filters + exclude specific target_keys
customer_enriched AS (
    SELECT
        cs.target_key,
        cs.total_orders,
        cs.total_sales,
        t.first_name,
        t.last_name,
        t.email
    FROM customer_sales cs
    JOIN ims.target    t
      ON cs.target_key = t.target_key
    WHERE t.email NOT LIKE '%surlatable%'
      AND t.email NOT LIKE '%SURLATABLE%'
      AND t.email NOT LIKE '%marketplace.amazon%'
      AND t.email NOT LIKE '%investcorp%'
      AND t.email IS NOT NULL
      AND t.email_valid_syntax = 1
      AND t.do_not_email <> 'Y'
      AND cs.target_key NOT IN (
          263490545,308997254,283621844,225456460,308055332,288280947,308529047,
          224718916,309609111,309687175,311851502,268565857,288517344,308556005,
          274699954,310052350,274931585,311734101,58835910,309388984,309524416,
          58703121,224601814,100014603941,226535975,201836045,217085172,
          310471577,311868423,264699141,312085250,309503161,311203453,277690814,
          263362940,35424472,309005898,309081049,189533733,287618034,308576748,
          269643148,309713993,275808870,308616198,310677692,312028513,296781601,
          275110000,224195656
      )
),

-- 8) Ranking and distribution metrics using window functions
ranked_customers AS (
    SELECT
        ce.*,
        ROW_NUMBER() OVER (ORDER BY ce.total_sales DESC)                AS row_num,
        RANK()       OVER (ORDER BY ce.total_sales DESC)                AS sales_rank,
        DENSE_RANK() OVER (ORDER BY ce.total_sales DESC)                AS sales_dense_rank,
        ce.total_sales * 1.0
            / NULLIF(SUM(ce.total_sales) OVER (), 0)                    AS sales_share,
        SUM(ce.total_sales) OVER (
            ORDER BY ce.total_sales DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        / NULLIF(SUM(ce.total_sales) OVER (), 0)                        AS cum_sales_share
    FROM customer_enriched ce
),

-- 9) Top 5000 customers by sales
top_5000 AS (
    SELECT *
    FROM ranked_customers
    WHERE row_num <= 5000
)

-- 10) Final result
SELECT
    target_key,
    first_name,
    last_name,
    email,
    total_orders,
    total_sales,
    sales_rank,
    sales_dense_rank,
    sales_share,
    cum_sales_share
FROM top_5000
ORDER BY total_sales DESC;
