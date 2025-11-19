/* 
--------------------------------------------------------------------------------
SQL Script: Store DSR KPI Full
--------------------------------------------------------------------------------
Objective:
    Generate daily Store KPIs across Sales, Forecast, Culinary Performance,
    Traffic, and Data Capture for Today and Yesterday, combining Delivered Sales,
    Delivered Returns, Forecast/Budget, Culinary Discounts, ShopperTrak traffic,
    and Customer Capture into a unified store-level dataset.

Definition:
    - Net Delivered Sales:
        • Delivered Sales minus Delivered Returns
        • Computed per store × date × channel (POS, BOPIS, Amazon, Walmart)

    - Forecast & Budget Integration:
        • Joins store-level forecast and budget tables by date, store, channel
        • Provides forecast/budget for sales, units, orders, and traffic

    - Culinary Net Sales:
        • Delivered Sales and Delivered Returns with Cooking discount codes
        • Produces net culinary dollars, units, and orders

    - Traffic (ShopperTrak):
        • Traffic Out count aggregated per store × date
        • Provided for both Today and 2-day windows

Processing Steps:
    1. Parameterize Today and Yesterday using a params CTE.
    2. Build Delivered Sales and Delivered Returns datasets (daily, store, channel).
    3. FULL OUTER JOIN Sales + Returns + Forecast/Budget using UNION logic.
    4. Aggregate store-level Net Dollars, Units, Orders, and Forecast/Budget KPIs.
    5. Build specialized KPIs:
         • Culinary Discount Net Sales
         • Culinary Forecast
         • ShopperTrak Traffic
         • Data Capture (email/address)
    6. Emulate FULL OUTER JOIN across all KPI groups using COALESCE matching logic.
    7. Produce a single combined KPI table with one row per store/manager/date.
    8. Output key metrics as WJXBFS-style fields for dashboard consumption.

Scope:
    - Includes only valid merchandising departments defined in the taxonomy list.
    - Includes POS, BOPIS, Walmart, Amazon, and OROMS channels.
    - Aggregated at: Store × Manager × Date; includes multiple KPI categories.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH
-- 1) Parameter CTE for dates
params AS (
    SELECT
        DATE('2025-11-17') AS as_of_date,        -- "today"
        DATE('2025-11-16') AS prev_date          -- "yesterday"
),

-- 2) BASE SALES / RETURNS / FORECAST – daily net by store + channel

grouped_sales AS (
    SELECT
        s.date_analytics_id,
        DATE(s.dt) AS gregorian_date,
        l.attribution_location_analytics_id AS location_analytics_id,
        COALESCE(l.sales_channel, 'pos')     AS sales_channel,
        SUM(s.merchandise)                  AS delivered_sales_dollars,
        SUM(s.quantity)                     AS delivered_sales_units,
        COUNT(DISTINCT s.order_analytics_id) AS delivered_sales_orders
    FROM peep_delivered_sales s
    JOIN peep_sales_line l
      ON l.order_line_analytics_id = s.order_line_analytics_id
    JOIN peep_product_to_merchandising_taxonomy mt
      ON s.product_analytics_id = mt.product_analytics_id
    JOIN (
        SELECT taxonomy_analytics_id AS department_id
        FROM peep_merchandising_taxonomies
        WHERE level = 3
          AND taxonomy_analytics_id IN
              (500004,500010,6,250007,500012,500005,8,250003,3,
               500006,250004,500007,250005,500008)
    ) dept
      ON mt.level_3_analytics_id = dept.department_id
    GROUP BY
        s.date_analytics_id,
        DATE(s.dt),
        l.attribution_location_analytics_id,
        COALESCE(l.sales_channel, 'pos')
),

grouped_returns AS (
    /* Returns to DC + in-store / BORIS / BOPISRO, unified */
    SELECT
        r.date_analytics_id,
        cal.gregorian_date,
        CASE
            WHEN r.location_analytics_id = 904
                 AND line.sales_channel NOT IN ('pos') THEN 2
            ELSE r.location_analytics_id
        END AS location_analytics_id,
        COALESCE(oroms_line.sales_channel, line.sales_channel, 'pos') AS sales_channel,
        SUM(r.merchandise)       AS delivered_returns_dollars,
        SUM(r.quantity)          AS delivered_returns_units,
        COUNT(DISTINCT r.order_analytics_id) AS delivered_returns_orders
    FROM peep_delivered_returns r
    JOIN peep_sales_line line
      ON r.order_line_analytics_id = line.order_line_analytics_id
    JOIN peep_sales_header h
      ON line.order_analytics_id = h.order_analytics_id
    JOIN common_calendar cal
      ON cal.date_analytics_id = r.date_analytics_id
    JOIN peep_product_to_merchandising_taxonomy mt
      ON r.product_analytics_id = mt.product_analytics_id
    JOIN peep_locations st
      ON st.location_analytics_id = line.attribution_location_analytics_id
    JOIN (
        SELECT taxonomy_analytics_id AS department_id
        FROM peep_merchandising_taxonomies
        WHERE level = 3
          AND taxonomy_analytics_id IN
              (500004,500010,6,250007,500012,500005,8,250003,3,
               500006,250004,500007,250005,500008)
    ) dept
      ON mt.level_3_analytics_id = dept.department_id
    LEFT JOIN peep_sales_header oroms_h
      ON r.original_order_analytics_id = oroms_h.order_analytics_id
     AND oroms_h.source = 'oroms'
    LEFT JOIN (
        SELECT
            order_analytics_id,
            product_analytics_id,
            MAX(sales_channel) AS sales_channel
        FROM peep_sales_line
        WHERE source = 'oroms'
        GROUP BY order_analytics_id, product_analytics_id
    ) oroms_line
      ON oroms_line.order_analytics_id = oroms_h.order_analytics_id
     AND oroms_line.product_analytics_id = r.product_analytics_id
    WHERE (mt.level_3_analytics_id IN
              (500004,500010,6,250007,500012,500005,8,250003,3,
               500006,250004,500007,250005,500008))
    GROUP BY
        r.date_analytics_id,
        cal.gregorian_date,
        CASE
            WHEN r.location_analytics_id = 904
                 AND line.sales_channel NOT IN ('pos') THEN 2
            ELSE r.location_analytics_id
        END,
        COALESCE(oroms_line.sales_channel, line.sales_channel, 'pos')
),

store_forecast_budget AS (
    SELECT
        fbv.location_analytics_id AS location_analytics_id,
        fbv.date_analytics_id,
        DATE(fbv.effective_date)  AS gregorian_date,
        fbv.sales_channel,
        SUM(COALESCE(fbv.forecast_sales,0))  AS forecast_sales,
        SUM(COALESCE(fbv.budget_sales,0))    AS budget_sales,
        SUM(COALESCE(fbv.forecast_units,0))  AS forecast_units,
        SUM(COALESCE(fbv.budget_units,0))    AS budget_units,
        SUM(COALESCE(fbv.forecast_orders,0)) AS forecast_orders,
        SUM(COALESCE(fbv.budget_orders,0))   AS budget_orders,
        SUM(COALESCE(fbv.forecast_traffic,0))AS forecast_traffic,
        SUM(COALESCE(fbv.budget_traffic,0))  AS budget_traffic
    FROM peep_forecast_and_budget_by_channel fbv
    GROUP BY
        fbv.location_analytics_id,
        fbv.date_analytics_id,
        DATE(fbv.effective_date),
        fbv.sales_channel
),

store_net_sales AS (
    -- emulate FULL OUTER JOIN of sales + returns + forecast
    SELECT
        COALESCE(s.date_analytics_id, r.date_analytics_id, f.date_analytics_id) AS date_analytics_id,
        COALESCE(s.gregorian_date,   r.gregorian_date,   f.gregorian_date)      AS gregorian_date,
        COALESCE(s.location_analytics_id, r.location_analytics_id, f.location_analytics_id)
                                                                                AS location_analytics_id,
        COALESCE(s.sales_channel, r.sales_channel, f.sales_channel)             AS sales_channel,
        COALESCE(s.delivered_sales_dollars, 0) - COALESCE(r.delivered_returns_dollars, 0)
                                                                                AS delivered_net_dollars_actual,
        COALESCE(s.delivered_sales_units,   0) - COALESCE(r.delivered_returns_units,   0)
                                                                                AS delivered_net_units_actual,
        COALESCE(s.delivered_sales_orders,  0) + COALESCE(r.delivered_returns_orders, 0)
                                                                                AS delivered_orders_actual,
        COALESCE(f.forecast_sales, 0)      AS forecast_dollars,
        COALESCE(f.budget_sales,  0)      AS budget_dollars,
        COALESCE(f.forecast_units, 0)     AS forecast_units,
        COALESCE(f.budget_units,  0)      AS budget_units,
        COALESCE(f.forecast_orders,0)     AS forecast_orders,
        COALESCE(f.budget_orders, 0)      AS budget_orders
    FROM grouped_sales s
    LEFT JOIN grouped_returns r
      ON r.date_analytics_id      = s.date_analytics_id
     AND r.location_analytics_id  = s.location_analytics_id
     AND r.sales_channel          = s.sales_channel
    LEFT JOIN store_forecast_budget f
      ON f.location_analytics_id  = COALESCE(s.location_analytics_id, r.location_analytics_id)
     AND f.date_analytics_id      = COALESCE(s.date_analytics_id, r.date_analytics_id)
     AND f.sales_channel          = COALESCE(s.sales_channel, r.sales_channel)

    UNION ALL

    SELECT
        COALESCE(r.date_analytics_id, f.date_analytics_id) AS date_analytics_id,
        COALESCE(r.gregorian_date,   f.gregorian_date)     AS gregorian_date,
        COALESCE(r.location_analytics_id, f.location_analytics_id)
                                                            AS location_analytics_id,
        COALESCE(r.sales_channel, f.sales_channel)         AS sales_channel,
        - COALESCE(r.delivered_returns_dollars,  0)        AS delivered_net_dollars_actual,
        - COALESCE(r.delivered_returns_units,    0)        AS delivered_net_units_actual,
        COALESCE(r.delivered_returns_orders,     0)        AS delivered_orders_actual,
        COALESCE(f.forecast_sales, 0)                      AS forecast_dollars,
        COALESCE(f.budget_sales,  0)                      AS budget_dollars,
        COALESCE(f.forecast_units, 0)                     AS forecast_units,
        COALESCE(f.budget_units,  0)                      AS budget_units,
        COALESCE(f.forecast_orders,0)                     AS forecast_orders,
        COALESCE(f.budget_orders, 0)                      AS budget_orders
    FROM grouped_returns r
    LEFT JOIN grouped_sales s
      ON s.date_analytics_id      = r.date_analytics_id
     AND s.location_analytics_id  = r.location_analytics_id
     AND s.sales_channel          = r.sales_channel
    LEFT JOIN store_forecast_budget f
      ON f.location_analytics_id  = r.location_analytics_id
     AND f.date_analytics_id      = r.date_analytics_id
     AND f.sales_channel          = r.sales_channel
    WHERE s.date_analytics_id IS NULL

    UNION ALL

    SELECT
        f.date_analytics_id,
        f.gregorian_date,
        f.location_analytics_id,
        f.sales_channel,
        0 AS delivered_net_dollars_actual,
        0 AS delivered_net_units_actual,
        0 AS delivered_orders_actual,
        f.forecast_sales  AS forecast_dollars,
        f.budget_sales    AS budget_dollars,
        f.forecast_units,
        f.budget_units,
        f.forecast_orders,
        f.budget_orders
    FROM store_forecast_budget f
    LEFT JOIN grouped_sales s
      ON s.date_analytics_id      = f.date_analytics_id
     AND s.location_analytics_id  = f.location_analytics_id
     AND s.sales_channel          = f.sales_channel
    LEFT JOIN grouped_returns r
      ON r.date_analytics_id      = f.date_analytics_id
     AND r.location_analytics_id  = f.location_analytics_id
     AND r.sales_channel          = f.sales_channel
    WHERE s.date_analytics_id IS NULL
      AND r.date_analytics_id IS NULL
),

sales_kpi AS (
    /* Aggregate store_net_sales to WJXBFS-style columns (TY, 1-day and 2-day windows) */
    SELECT
        loc.date_opened,
        loc.manager,
        s.location_analytics_id,
        -- Single-day KPIs (as_of_date)
        SUM(
            CASE WHEN DATE(s.gregorian_date) = p.as_of_date
                 THEN s.delivered_net_dollars_actual END
        )                                   AS WJXBFS1,   -- total net sales TY (day)
        SUM(
            CASE WHEN DATE(s.gregorian_date) = p.as_of_date
                 THEN s.forecast_dollars END
        )                                   AS WJXBFS2,   -- total forecast TY (day)
        MAX(
            CASE WHEN DATE(s.gregorian_date) = p.as_of_date
                 THEN 1 ELSE 0 END
        )                                   AS GODWFLAG1_1, -- has sales that day

        -- POS-only KPIs (exclude Amazon/BOPIS/Walmart)
        SUM(
            CASE WHEN COALESCE(s.sales_channel,'pos')
                      NOT IN ('amazon_delivery','amazon_pickup','slt_bopis','walmart_go_local')
                      AND DATE(s.gregorian_date) = p.as_of_date
                 THEN s.delivered_net_dollars_actual END
        )                                   AS WJXBFS3,
        SUM(
            CASE WHEN COALESCE(s.sales_channel,'pos')
                      NOT IN ('amazon_delivery','amazon_pickup','slt_bopis','walmart_go_local')
                      AND DATE(s.gregorian_date) = p.as_of_date
                 THEN s.forecast_dollars END
        )                                   AS WJXBFS4,

        -- 2-day window net sales (prev_date + as_of_date)
        SUM(
            CASE WHEN DATE(s.gregorian_date) BETWEEN p.prev_date AND p.as_of_date
                 THEN s.delivered_net_dollars_actual END
        )                                   AS WJXBFS12,
        SUM(
            CASE WHEN DATE(s.gregorian_date) BETWEEN p.prev_date AND p.as_of_date
                 THEN s.forecast_dollars END
        )                                   AS WJXBFS13
    FROM store_net_sales s
    JOIN params p
      ON 1=1
    JOIN locations loc
      ON loc.location_analytics_id = s.location_analytics_id
    JOIN calendar cal
      ON cal.date_analytics_id = s.date_analytics_id
    WHERE DATE(cal.gregorian_date) BETWEEN p.prev_date AND p.as_of_date
    GROUP BY
        loc.date_opened,
        loc.manager,
        s.location_analytics_id
),

-- 3) CULINARY SALES (COOKING% discounts) – net culinary dollars

culinary_sales AS (
    SELECT
        s.date_analytics_id,
        DATE(s.dt) AS gregorian_date,
        l.attribution_location_analytics_id AS location_analytics_id,
        SUM(s.merchandise)                  AS delivered_sales_dollars,
        SUM(s.quantity)                     AS delivered_sales_units,
        COUNT(DISTINCT s.order_analytics_id)AS delivered_sales_orders
    FROM peep_delivered_sales s
    LEFT JOIN peep_sales_line l
      ON l.order_line_analytics_id = s.order_line_analytics_id
    LEFT JOIN peep_pos_discounts dis
      ON dis.order_line_analytics_id = s.order_line_analytics_id
    WHERE dis.discount_code LIKE 'COOKING%'
    GROUP BY 1,2,3
),

culinary_returns AS (
    SELECT
        s.date_analytics_id,
        DATE(s.dt) AS gregorian_date,
        l.attribution_location_analytics_id AS location_analytics_id,
        SUM(s.merchandise)                  AS delivered_return_dollars,
        SUM(s.quantity)                     AS delivered_return_units,
        COUNT(DISTINCT s.order_analytics_id)AS delivered_return_orders
    FROM peep_delivered_returns s
    LEFT JOIN peep_sales_line l
      ON l.order_line_analytics_id = s.order_line_analytics_id
    LEFT JOIN peep_pos_discounts dis
      ON dis.order_line_analytics_id = s.order_line_analytics_id
    WHERE dis.discount_code LIKE 'COOKING%'
    GROUP BY 1,2,3
),

culinary_net AS (
    -- emulate FULL OUTER JOIN
    SELECT
        COALESCE(c.date_analytics_id, r.date_analytics_id)        AS date_analytics_id,
        COALESCE(c.gregorian_date,   r.gregorian_date)            AS gregorian_date,
        COALESCE(c.location_analytics_id, r.location_analytics_id)AS location_analytics_id,
        COALESCE(c.delivered_sales_dollars,0)
          - COALESCE(r.delivered_return_dollars,0)                AS delivered_net_culinary_dollars,
        COALESCE(c.delivered_sales_units,0)
          - COALESCE(r.delivered_return_units,0)                  AS delivered_net_culinary_units,
        COALESCE(c.delivered_sales_orders,0)
          + COALESCE(r.delivered_return_orders,0)                 AS culinary_orders
    FROM culinary_sales c
    LEFT JOIN culinary_returns r
      ON r.date_analytics_id = c.date_analytics_id
     AND r.location_analytics_id = c.location_analytics_id

    UNION ALL

    SELECT
        r.date_analytics_id,
        r.gregorian_date,
        r.location_analytics_id,
        - COALESCE(r.delivered_return_dollars,0),
        - COALESCE(r.delivered_return_units,0),
        COALESCE(r.delivered_return_orders,0)
    FROM culinary_returns r
    LEFT JOIN culinary_sales c
      ON c.date_analytics_id = r.date_analytics_id
     AND c.location_analytics_id = r.location_analytics_id
    WHERE c.date_analytics_id IS NULL
),

culinary_kpi AS (
    SELECT
        loc.date_opened,
        loc.manager,
        c.location_analytics_id,
        SUM(
            CASE WHEN DATE(c.gregorian_date) = p.as_of_date
                 THEN c.delivered_net_culinary_dollars END
        ) AS WJXBFS1,   -- net culinary TY (day)
        SUM(
            CASE WHEN DATE(c.gregorian_date) BETWEEN p.prev_date AND p.as_of_date
                 THEN c.delivered_net_culinary_dollars END
        ) AS WJXBFS2    -- net culinary TY (2-day)
    FROM culinary_net c
    JOIN params p
      ON 1=1
    JOIN locations loc
      ON loc.location_analytics_id = c.location_analytics_id
    JOIN calendar cal
      ON cal.date_analytics_id = c.date_analytics_id
    WHERE DATE(cal.gregorian_date) BETWEEN p.prev_date AND p.as_of_date
    GROUP BY
        loc.date_opened,
        loc.manager,
        c.location_analytics_id
),

-- 4) TRAFFIC KPI (ShopperTrak)

traffic_kpi AS (
    SELECT
        loc.date_opened,
        loc.manager,
        t.location_analytics_id,
        SUM(
            CASE WHEN DATE(cal.gregorian_date) = p.as_of_date
                 THEN t.traffic_out_count END
        ) AS WJXBFS1, -- traffic TY (day)
        SUM(
            CASE WHEN DATE(cal.gregorian_date) BETWEEN p.prev_date AND p.as_of_date
                 THEN t.traffic_out_count END
        ) AS WJXBFS2  -- traffic TY (2-day)
    FROM shoppertrak_traffic t
    JOIN locations loc
      ON loc.location_analytics_id = t.location_analytics_id
    JOIN calendar cal
      ON cal.date_analytics_id = t.date_analytics_id
    JOIN params p ON 1=1
    WHERE DATE(cal.gregorian_date) BETWEEN p.prev_date AND p.as_of_date
    GROUP BY
        loc.date_opened,
        loc.manager,
        t.location_analytics_id
),

-- 5) DATA CAPTURE KPI (store_kpi – address/email)

capture_kpi AS (
    SELECT
        loc.date_opened,
        loc.manager,
        k.location_analytics_id,
        SUM(
            CASE WHEN DATE(cal.gregorian_date) = p.as_of_date
                 THEN k.hasaddress END
        ) AS WJXBFS1,  -- addresses captures TY (day)
        SUM(
            CASE WHEN DATE(cal.gregorian_date) = p.as_of_date
                 THEN k.hasemail END
        ) AS WJXBFS2,  -- emails captures TY (day)
        SUM(
            CASE WHEN DATE(cal.gregorian_date) BETWEEN p.prev_date AND p.as_of_date
                 THEN k.hasaddress END
        ) AS WJXBFS3,  -- addresses TY (2-day)
        SUM(
            CASE WHEN DATE(cal.gregorian_date) BETWEEN p.prev_date AND p.as_of_date
                 THEN k.hasemail END
        ) AS WJXBFS4   -- emails TY (2-day)
    FROM store_kpi k
    JOIN locations loc
      ON loc.location_analytics_id = k.location_analytics_id
    JOIN calendar cal
      ON cal.date_analytics_id = k.date_analytics_id
    JOIN params p ON 1=1
    WHERE DATE(cal.gregorian_date) BETWEEN p.prev_date AND p.as_of_date
    GROUP BY
        loc.date_opened,
        loc.manager,
        k.location_analytics_id
),

-- 6) CULINARY FORECAST KPI (forecast_and_budget.culinary_discount_forecast)

culinary_fcst_kpi AS (
    SELECT
        loc.date_opened,
        loc.manager,
        fb.location_analytics_id,
        SUM(
            CASE WHEN DATE(cal.gregorian_date) = p.as_of_date
                 THEN fb.culinary_discount_forecast END
        ) AS WJXBFS1, -- culinary FCST TY (day)
        SUM(
            CASE WHEN DATE(cal.gregorian_date) BETWEEN p.prev_date AND p.as_of_date
                 THEN fb.culinary_discount_forecast END
        ) AS WJXBFS2  -- culinary FCST TY (2-day)
    FROM forecast_and_budget fb
    JOIN locations loc
      ON loc.location_analytics_id = fb.location_analytics_id
    JOIN calendar cal
      ON cal.date_analytics_id = fb.date_analytics_id
    JOIN params p ON 1=1
    WHERE DATE(cal.gregorian_date) BETWEEN p.prev_date AND p.as_of_date
    GROUP BY
        loc.date_opened,
        loc.manager,
        fb.location_analytics_id
),

-- 7) FINAL COMBINED KPI TABLE – emulate FULL OUTER JOIN across KPI sets
combined_kpi AS (
    SELECT
        COALESCE(s.date_opened, c.date_opened, t.date_opened, cap.date_opened, cf.date_opened) AS date_opened,
        COALESCE(s.manager,     c.manager,     t.manager,     cap.manager,     cf.manager)     AS manager,
        COALESCE(s.location_analytics_id, c.location_analytics_id,
                 t.location_analytics_id, cap.location_analytics_id,
                 cf.location_analytics_id) AS location_analytics_id,

        -- SALES
        s.WJXBFS1 AS sales_day_net_dollars,
        s.WJXBFS2 AS sales_day_forecast_dollars,
        s.WJXBFS12 AS sales_2day_net_dollars,
        s.WJXBFS13 AS sales_2day_forecast_dollars,

        -- CULINARY
        c.WJXBFS1 AS culinary_day_net_dollars,
        c.WJXBFS2 AS culinary_2day_net_dollars,

        -- TRAFFIC
        t.WJXBFS1 AS traffic_day,
        t.WJXBFS2 AS traffic_2day,

        -- DATA CAPTURE
        cap.WJXBFS1 AS address_day,
        cap.WJXBFS2 AS email_day,
        cap.WJXBFS3 AS address_2day,
        cap.WJXBFS4 AS email_2day,

        -- CULINARY FORECAST
        cf.WJXBFS1 AS culinary_fcst_day,
        cf.WJXBFS2 AS culinary_fcst_2day
    FROM sales_kpi s
    LEFT JOIN culinary_kpi c
      ON c.location_analytics_id = s.location_analytics_id
     AND c.manager               = s.manager
     AND c.date_opened           = s.date_opened
    LEFT JOIN traffic_kpi t
      ON t.location_analytics_id = COALESCE(s.location_analytics_id, c.location_analytics_id)
     AND t.manager               = COALESCE(s.manager, c.manager)
     AND t.date_opened           = COALESCE(s.date_opened, c.date_opened)
    LEFT JOIN capture_kpi cap
      ON cap.location_analytics_id = COALESCE(s.location_analytics_id, c.location_analytics_id, t.location_analytics_id)
     AND cap.manager               = COALESCE(s.manager, c.manager, t.manager)
     AND cap.date_opened           = COALESCE(s.date_opened, c.date_opened, t.date_opened)
    LEFT JOIN culinary_fcst_kpi cf
      ON cf.location_analytics_id = COALESCE(s.location_analytics_id, c.location_analytics_id,
                                             t.location_analytics_id, cap.location_analytics_id)
     AND cf.manager               = COALESCE(s.manager, c.manager, t.manager, cap.manager)
     AND cf.date_opened           = COALESCE(s.date_opened, c.date_opened, t.date_opened, cap.date_opened)

    UNION ALL

    -- Add locations that appear only in culinary_kpi, traffic_kpi, capture_kpi, or culinary_fcst_kpi
    SELECT
        c.date_opened,
        c.manager,
        c.location_analytics_id,
        NULL, NULL, NULL, NULL,
        c.WJXBFS1, c.WJXBFS2,
        NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL, NULL
    FROM culinary_kpi c
    LEFT JOIN sales_kpi s
      ON s.location_analytics_id = c.location_analytics_id
     AND s.manager               = c.manager
     AND s.date_opened           = c.date_opened
    WHERE s.location_analytics_id IS NULL
)

-- FINAL SELECT
SELECT
    date_opened,
    manager,
    location_analytics_id,

    sales_day_net_dollars          AS WJXBFS1,
    sales_day_forecast_dollars     AS WJXBFS2,
    sales_2day_net_dollars         AS WJXBFS12,
    sales_2day_forecast_dollars    AS WJXBFS13,

    culinary_day_net_dollars       AS WJXBFS_CUL_DAY,
    culinary_2day_net_dollars      AS WJXBFS_CUL_2DAY,

    traffic_day                    AS WJXBFS_TRAFFIC_DAY,
    traffic_2day                   AS WJXBFS_TRAFFIC_2DAY,

    address_day                    AS WJXBFS_ADDR_DAY,
    email_day                      AS WJXBFS_EMAIL_DAY,
    address_2day                   AS WJXBFS_ADDR_2DAY,
    email_2day                     AS WJXBFS_EMAIL_2DAY,

    culinary_fcst_day              AS WJXBFS_CUL_FCST_DAY,
    culinary_fcst_2day             AS WJXBFS_CUL_FCST_2DAY
FROM combined_kpi
ORDER BY manager, location_analytics_id;
