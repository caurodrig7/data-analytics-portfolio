/* 
--------------------------------------------------------------------------------
SQL Script: Top Cooking Classes – Overall, New vs Existing Customers
--------------------------------------------------------------------------------
Objective:
    Produce a class-level view of Cooking School performance for a selected year:
        • Ranked sales and quantity by class.
        • Segmented results for: All Customers, New Customers, Existing Customers
        • Customer counts, order counts, sales shares, and cumulative sales curves.

    Results feed Cooking School dashboards to evaluate:
        • Which classes drive the most revenue.
        • How class performance differs for new vs repeat customers.
        • Cross-customer engagement trends for culinary programs.

Definition:
    - Base Orders:
        • Cooking School orders only (positive revenue, real customers, purchase_order = 'Y').
        • Filtered to a single analysis year.
        • Enriched with customer tenure using original_entered_date.

    - Base Order Lines:
        • Positive-revenue IMS order_line rows mapped to products.
        • Joined to class names via ims.product.

    - Customer Tenure:
        • “New” customers: original_entered_date year = analysis year.
        • “Existing” customers: customer created prior to analysis year.
        • “All” is added through a roll-up grouping.

    - Class Metrics:
        • Sales and quantity at the class level.
        • Distinct customers and distinct orders per class.
        • Sales share and quantity share by tenure group.
        • Ranking of classes by sales within each tenure.
        • Cumulative sales share (Pareto curve) within each tenure.

Scope:
    - Cooking School category only (product.category_name = 'COOKING SCHOOL').
    - Customer-level segmentation based on first-year vs prior-year acquisition.
    - Reporting grain: Class × Customer Tenure Group (All, New, Existing).

Processing Steps:
    1. Build params CTE storing analysis year.
    2. Build base_orders CTE filtering valid Cooking School orders.
    3. Build base_lines CTE for positive order lines.
    4. Build customer_dim CTE assigning New vs Existing tenure.
    5. Build order_class_facts combining orders, lines, products, and customers.
    6. Build class_tenure_agg summarizing:
           • sales, quantity, distinct customers, distinct orders by class × tenure.
           • Generate “All” roll-up using GROUPING SETS.
    7. Build class_metrics applying window functions to compute:
           • sales_by_tenure, qty_by_tenure
           • sales share, quantity share
           • ranking and cumulative sales share
    8. Build class_metrics_labeled adding “Top 10” flag by tenure.
    9. Final output: ordered class ranking for All, New, and Existing customer groups.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/


WITH
-- 1) Parameters
params AS (
    SELECT
        2025       AS order_year_filter,
        '2025'     AS new_customer_year_str
),

-- 2) Base valid orders for the year (Cooking School only, real customers)
base_orders AS (
    SELECT
        oh.order_number,
        oh.target_key,
        oh.order_year,
        oh.net_amount,
        oh.cooking_school_amount
    FROM ims.order_header oh
    JOIN params p
      ON oh.order_year = p.order_year_filter
    WHERE oh.purchase_order = 'Y'
      AND oh.target_key   <> -1
      AND oh.net_amount    > 0
      AND oh.cooking_school_amount > 0
),

-- 3) Base order lines with positive revenue
base_lines AS (
    SELECT
        ol.order_number,
        ol.item_number,
        ol.net_amount   AS line_net_amount,
        ol.net_quantity AS line_quantity
    FROM ims.order_line ol
    WHERE ol.net_amount > 0
),

-- 4) Customer dimension with tenure flag
customer_dim AS (
    SELECT
        t.target_key,
        t.original_entered_date,
        CASE
            WHEN TO_CHAR(t.original_entered_date, 'YYYY')
                     = (SELECT new_customer_year_str FROM params)
            THEN 'New'
            ELSE 'Existing'
        END AS customer_tenure
    FROM ims.target t
),

-- 5) Join orders, lines, products, and customers – restrict to Cooking School
order_class_facts AS (
    SELECT
        bo.order_number,
        bo.target_key,
        bo.order_year,
        bl.item_number,
        bl.line_net_amount,
        bl.line_quantity,
        p.class_name,
        cd.customer_tenure
    FROM base_orders      bo
    JOIN base_lines       bl  ON bo.order_number = bl.order_number
    JOIN ims.product      p   ON bl.item_number  = p.sku
    JOIN customer_dim     cd  ON bo.target_key   = cd.target_key
    WHERE p.category_name = 'COOKING SCHOOL'
),

-- 6) Aggregate by class × tenure, and add an "All Tenures" roll-up
class_tenure_agg AS (
    SELECT
        CASE
            WHEN GROUPING(customer_tenure) = 1 THEN 'All'
            ELSE customer_tenure
        END                                   AS customer_tenure_group,
        class_name,
        SUM(line_net_amount)                  AS sales,
        SUM(line_quantity)                    AS quantity,
        COUNT(DISTINCT target_key)            AS distinct_customers,
        COUNT(DISTINCT order_number)          AS distinct_orders
    FROM order_class_facts
    GROUP BY GROUPING SETS (
        (class_name, customer_tenure),
        (class_name)
    )
),

-- 7) Add window-based KPIs: total sales/qty, shares, ranking, cumulative curves
class_metrics AS (
    SELECT
        cta.*,

        -- Totals per tenure group
        SUM(sales)    OVER (PARTITION BY customer_tenure_group) AS sales_by_tenure,
        SUM(quantity) OVER (PARTITION BY customer_tenure_group) AS qty_by_tenure,

        -- Overall totals across all tenure groups
        SUM(sales)    OVER () AS sales_all_groups,
        SUM(quantity) OVER () AS qty_all_groups,

        -- Share of tenure-level sales & quantity
        CASE
            WHEN SUM(sales) OVER (PARTITION BY customer_tenure_group) = 0
                THEN 0
            ELSE sales * 1.0
                 / NULLIF(SUM(sales) OVER (PARTITION BY customer_tenure_group), 0)
        END AS sales_share_within_tenure,

        CASE
            WHEN SUM(quantity) OVER (PARTITION BY customer_tenure_group) = 0
                THEN 0
            ELSE quantity * 1.0
                 / NULLIF(SUM(quantity) OVER (PARTITION BY customer_tenure_group), 0)
        END AS qty_share_within_tenure,

        -- Rank classes within each tenure group by sales
        RANK() OVER (
            PARTITION BY customer_tenure_group
            ORDER BY sales DESC
        ) AS sales_rank_within_tenure,

        -- Cumulative sales share within each tenure group (Pareto-style)
        SUM(sales) OVER (
            PARTITION BY customer_tenure_group
            ORDER BY sales DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
        / NULLIF(
            SUM(sales) OVER (PARTITION BY customer_tenure_group),
            0
        ) AS cum_sales_share_within_tenure
    FROM class_tenure_agg cta
),

-- 8) Optional subquery to flag "Top 10 by tenure" based on rank
class_metrics_labeled AS (
    SELECT
        cm.*,
        CASE
            WHEN cm.sales_rank_within_tenure <= 10 THEN 'Top 10'
            ELSE 'Other'
        END AS top_flag
    FROM class_metrics cm
)

-- 9) Final result
SELECT
    customer_tenure_group   AS customer_tenure,
    class_name              AS class_name,
    sales,
    quantity,
    distinct_customers,
    distinct_orders,
    sales_by_tenure,
    qty_by_tenure,
    sales_all_groups,
    qty_all_groups,
    sales_share_within_tenure,
    qty_share_within_tenure,
    sales_rank_within_tenure,
    cum_sales_share_within_tenure,
    top_flag
FROM class_metrics_labeled
ORDER BY
    CASE customer_tenure_group
        WHEN 'All'      THEN 0
        WHEN 'New'      THEN 1
        WHEN 'Existing' THEN 2
        ELSE 3
    END,
    sales_rank_within_tenure;
