/* 
--------------------------------------------------------------------------------
SQL Script: Planning Dashboard – Inventory Position by Location Type
--------------------------------------------------------------------------------
Objective:
    Produce a unified, channel-level inventory position dataset for the Planning
    Dashboard, combining current inventory, prior-week inventory, last-year
    inventory, in-transit inventory, and store-count metrics. Enable planners,
    merchants, and finance partners to understand inventory trends, YoY/WoW
    performance, and inventory efficiency across stores, e-commerce, and
    fulfillment channels.

Definition:
    - Current Inventory (WJXBFS1, WJXBFS3):
        • Fair-Market-Value (FMV) available inventory on the as_of_date.
        • In-transit FMV for the same as_of_date.
        • Derived from product_history, calendar, and location channel mapping.

    - Last-Year Inventory (WJXBFS2):
        • FMV from last_year_date_analytics_id aligned to the same as_of_date.
        • Allows clean YoY comparisons at the channel level.

    - Prior-Week Inventory (WJXBFS4):
        • FMV inventory from the date 7 days prior to as_of_date.
        • Used for WoW movement analysis.

    - Store Counts (WJXBFS5, WJXBFS6):
        • Count of stores with delivered sales > 0 for the defined comp week.
        • This-year counts based on current fiscal week.
        • Last-year counts based on same fiscal week using last_year_date mapping.

    - Advanced KPIs (added via window functions):
        • Total current inventory across all channels.
        • Channel share of total inventory.
        • YoY inventory change (absolute + %).
        • WoW inventory change (absolute + %).
        • Inventory per active store.
        • Channel ranking by inventory magnitude.

Scope:
    - Includes only Level-3 merchandising departments:
        500004, 500010, 6, 250007, 500012, 500005, 8,
        250003, 3, 500006, 250004, 500007, 250005, 500008.
    - Includes only channels: Store (1), Ecommerce (2), Fulfillment (5).
    - Applies filters for current snapshot, prior-week snapshot, and last-year
      comp week using calendar mappings.
    - Aggregates via CTE chains rather than temporary tables.
    - Outputs one row per channel with all inventory-position measures.

Processing Steps:
    1. Define parameters for as_of_date, prior-week snapshot, and comp-week dates.
    2. Build inventory_base CTE: FMV by date × product × channel.
    3. Roll up inventory totals and separate snapshots (current + prior-week).
    4. Compute current inventory snapshot (available + in-transit) per channel.
    5. Compute last-year inventory snapshot mapped via last_year_date_analytics_id.
    6. Compute prior-week inventory by matching prior_week_date.
    7. Build delivered-sales daily store counts for both TY and LY comp weeks.
    8. Aggregate store counts to most recent day of comp week (TY + LY).
    9. Generate complete channel set using FULL OUTER JOIN emulation via UNION.
    10. Merge all inventory + store-count facts into a single dataset.
    11. Apply window calculations:
          • Sum of current inventory across channels.
          • Channel share percentages.
          • YoY and WoW % changes.
          • Inventory per store.
          • Ranking by inventory across all channels.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH
-- 0) Parameter CTE – centralize all date settings
params AS (
    SELECT
        DATE('2025-11-16') AS as_of_date,                  -- current inventory snapshot
        DATE('2025-11-09') AS prior_week_date,             -- prior-week inventory snapshot
        DATE('2025-11-09') AS stores_week_start,           -- week for store counts (current year)
        DATE('2025-11-15') AS stores_week_end,
        DATE('2025-11-09') AS stores_week_start_ly,        -- comp week last year (mapped via calendar)
        DATE('2025-11-15') AS stores_week_end_ly
),

-- 1) Inventory base – product_history + merch + locations + calendar
inventory_base AS (
    SELECT
        ph.date_analytics_id,
        loc.channel_analytics_id AS channel,
        cal.gregorian_date,
        ph.available_fair_market_value,
        ph.in_transit_fair_market_value
    FROM product_history ph
    JOIN locations loc
      ON ph.location_analytics_id = loc.location_analytics_id
    JOIN product_to_merchandising_taxonomy pmt
      ON ph.product_analytics_id = pmt.product_analytics_id
    JOIN calendar cal
      ON ph.date_analytics_id = cal.date_analytics_id
    JOIN params p
      ON DATE(cal.gregorian_date) IN (p.as_of_date, p.prior_week_date)
    WHERE pmt.level_3_analytics_id IN (
        500004, 500010, 6, 250007, 500012, 500005, 8,
        250003, 3, 500006, 250004, 500007, 250005, 500008
    )
      AND loc.channel_analytics_id IN ('1','2','5')
),

inventory_rollup AS (
    SELECT
        ib.date_analytics_id,
        ib.channel,
        -- Current-day inventory (as_of_date)
        SUM(
            CASE WHEN DATE(ib.gregorian_date) = (SELECT as_of_date FROM params)
                 THEN ib.available_fair_market_value
                 ELSE NULL
            END
        ) AS curr_available_fmv,
        SUM(
            CASE WHEN DATE(ib.gregorian_date) = (SELECT as_of_date FROM params)
                 THEN ib.in_transit_fair_market_value
                 ELSE NULL
            END
        ) AS curr_in_transit_fmv,
        MAX(
            CASE WHEN DATE(ib.gregorian_date) = (SELECT as_of_date FROM params)
                 THEN 1 ELSE 0
            END
        ) AS flag_curr,
        -- Prior-week inventory (prior_week_date)
        SUM(
            CASE WHEN DATE(ib.gregorian_date) = (SELECT prior_week_date FROM params)
                 THEN ib.available_fair_market_value
                 ELSE NULL
            END
        ) AS prior_week_available_fmv,
        MAX(
            CASE WHEN DATE(ib.gregorian_date) = (SELECT prior_week_date FROM params)
                 THEN 1 ELSE 0
            END
        ) AS flag_prior_week
    FROM inventory_base ib
    GROUP BY
        ib.date_analytics_id,
        ib.channel
),

-- 2) Current inventory snapshot by channel
curr_inv_max_date AS (
    SELECT
        channel,
        MAX(date_analytics_id) AS max_date_analytics_id
    FROM inventory_rollup
    WHERE flag_curr = 1
    GROUP BY channel
),

curr_inventory_by_channel AS (
    SELECT DISTINCT
        ir.channel,
        ir.curr_available_fmv      AS current_available_fmv,
        ir.curr_in_transit_fmv     AS current_in_transit_fmv
    FROM inventory_rollup ir
    JOIN curr_inv_max_date md
      ON ir.channel = md.channel
     AND ir.date_analytics_id = md.max_date_analytics_id
    WHERE ir.flag_curr = 1
),

-- 3) Last-year inventory snapshot for as_of_date 
ly_inventory_rollup AS (
    SELECT
        cal.date_analytics_id,
        loc.channel_analytics_id AS channel,
        SUM(ph.available_fair_market_value) AS ly_available_fmv
    FROM product_history ph
    JOIN calendar cal
      ON ph.date_analytics_id = cal.last_year_date_analytics_id
    JOIN locations loc
      ON ph.location_analytics_id = loc.location_analytics_id
    JOIN product_to_merchandising_taxonomy pmt
      ON ph.product_analytics_id = pmt.product_analytics_id
    JOIN params p
      ON DATE(cal.gregorian_date) = p.as_of_date
    WHERE pmt.level_3_analytics_id IN (
        500004, 500010, 6, 250007, 500012, 500005, 8,
        250003, 3, 500006, 250004, 500007, 250005, 500008
    )
      AND loc.channel_analytics_id IN ('1','2','5')
    GROUP BY
        cal.date_analytics_id,
        loc.channel_analytics_id
),

ly_inv_max_date AS (
    SELECT
        channel,
        MAX(date_analytics_id) AS max_date_analytics_id
    FROM ly_inventory_rollup
    GROUP BY channel
),

ly_inventory_by_channel AS (
    SELECT DISTINCT
        lyr.channel,
        lyr.ly_available_fmv
    FROM ly_inventory_rollup lyr
    JOIN ly_inv_max_date md
      ON lyr.channel = md.channel
     AND lyr.date_analytics_id = md.max_date_analytics_id
),

-- 4) Prior-week inventory snapshot by channel 
prior_week_inv_max_date AS (
    SELECT
        channel,
        MAX(date_analytics_id) AS max_date_analytics_id
    FROM inventory_rollup
    WHERE flag_prior_week = 1
    GROUP BY channel
),

prior_week_inventory_by_channel AS (
    SELECT DISTINCT
        ir.channel,
        ir.prior_week_available_fmv AS prior_week_available_fmv
    FROM inventory_rollup ir
    JOIN prior_week_inv_max_date md
      ON ir.channel = md.channel
     AND ir.date_analytics_id = md.max_date_analytics_id
    WHERE ir.flag_prior_week = 1
),

-- 5) Delivered-sales daily store counts – reusable DS base
delivered_sales_daily AS (
    SELECT
        ds.date_analytics_id,
        DATE(ds.dt)                   AS gregorian_date,
        sl.attribution_location_analytics_id,
        SUM(ds.merchandise)          AS delivered_gross_sales_dollars,
        CASE
            WHEN SUM(ds.merchandise) > 0 THEN 1
            ELSE 0
        END AS store_count
    FROM peep.delivered_sales ds
    JOIN peep.sales_line sl
      ON sl.order_line_analytics_id = ds.order_line_analytics_id
    WHERE sl.attribution_location_analytics_id <> 2
    GROUP BY
        ds.date_analytics_id,
        DATE(ds.dt),
        sl.attribution_location_analytics_id
),

-- 6) Store counts for this year – weekly 
stores_weekly_curr AS (
    SELECT
        ds.date_analytics_id,
        loc.channel_analytics_id AS channel,
        SUM(ds.store_count)      AS store_count_for_day
    FROM delivered_sales_daily ds
    JOIN locations loc
      ON ds.attribution_location_analytics_id = loc.location_analytics_id
    JOIN calendar cal
      ON ds.date_analytics_id = cal.date_analytics_id
    JOIN params p
      ON cal.fiscal_week_id IN (
            SELECT c2.fiscal_week_id
            FROM calendar c2
            WHERE DATE(c2.gregorian_date)
                  BETWEEN p.stores_week_start AND p.stores_week_end
         )
    WHERE loc.channel_analytics_id IN ('1','2','5')
    GROUP BY
        ds.date_analytics_id,
        loc.channel_analytics_id
),

stores_curr_max_date AS (
    SELECT
        channel,
        MAX(date_analytics_id) AS max_date_analytics_id
    FROM stores_weekly_curr
    GROUP BY channel
),

stores_curr_by_channel AS (
    SELECT DISTINCT
        swc.channel,
        swc.store_count_for_day AS current_store_count
    FROM stores_weekly_curr swc
    JOIN stores_curr_max_date md
      ON swc.channel = md.channel
     AND swc.date_analytics_id = md.max_date_analytics_id
),

-- 7) Store counts for last year – comp week
stores_weekly_ly AS (
    SELECT
        cal.date_analytics_id,
        loc.channel_analytics_id AS channel,
        SUM(ds.store_count)      AS store_count_for_day
    FROM delivered_sales_daily ds
    JOIN calendar cal
      ON ds.date_analytics_id = cal.last_year_date_analytics_id
    JOIN locations loc
      ON ds.attribution_location_analytics_id = loc.location_analytics_id
    JOIN params p
      ON DATE(cal.gregorian_date)
         BETWEEN p.stores_week_start AND p.stores_week_end
    WHERE loc.channel_analytics_id IN ('1','2','5')
    GROUP BY
        cal.date_analytics_id,
        loc.channel_analytics_id
),

stores_ly_max_date AS (
    SELECT
        channel,
        MAX(date_analytics_id) AS max_date_analytics_id
    FROM stores_weekly_ly
    GROUP BY channel
),

stores_ly_by_channel AS (
    SELECT DISTINCT
        swl.channel,
        swl.store_count_for_day AS ly_store_count
    FROM stores_weekly_ly swl
    JOIN stores_ly_max_date md
      ON swl.channel = md.channel
     AND swl.date_analytics_id = md.max_date_analytics_id
),

-- 8) Union of all channels present in any component 
all_channels AS (
    SELECT channel FROM curr_inventory_by_channel
    UNION
    SELECT channel FROM ly_inventory_by_channel
    UNION
    SELECT channel FROM prior_week_inventory_by_channel
    UNION
    SELECT channel FROM stores_curr_by_channel
    UNION
    SELECT channel FROM stores_ly_by_channel
),

-- 9) Merge all measures by channel
merged_channel_metrics AS (
    SELECT
        ac.channel,
        ci.current_available_fmv,
        ly.ly_available_fmv,
        ci.current_in_transit_fmv,
        pw.prior_week_available_fmv,
        sc.current_store_count,
        sly.ly_store_count
    FROM all_channels ac
    LEFT JOIN curr_inventory_by_channel ci
      ON ac.channel = ci.channel
    LEFT JOIN ly_inventory_by_channel ly
      ON ac.channel = ly.channel
    LEFT JOIN prior_week_inventory_by_channel pw
      ON ac.channel = pw.channel
    LEFT JOIN stores_curr_by_channel sc
      ON ac.channel = sc.channel
    LEFT JOIN stores_ly_by_channel sly
      ON ac.channel = sly.channel
),

-- 10) Add advanced metrics
final_with_windows AS (
    SELECT
        m.channel,
        m.current_available_fmv           AS WJXBFS1,
        m.ly_available_fmv                AS WJXBFS2,
        m.current_in_transit_fmv          AS WJXBFS3,
        m.prior_week_available_fmv        AS WJXBFS4,
        m.current_store_count             AS WJXBFS5,
        m.ly_store_count                  AS WJXBFS6,

        -- total current available across all channels
        SUM(m.current_available_fmv) OVER () AS total_curr_avail_all_channels,

        -- channel share of current available inventory
        m.current_available_fmv
        / NULLIF(SUM(m.current_available_fmv) OVER (), 0)
            AS curr_avail_share_of_total,

        -- YoY inventory change and % change
        (m.current_available_fmv - m.ly_available_fmv) AS yoy_change_available_fmv,
        CASE
            WHEN m.ly_available_fmv IS NULL OR m.ly_available_fmv = 0 THEN NULL
            ELSE (m.current_available_fmv - m.ly_available_fmv) / m.ly_available_fmv
        END AS yoy_change_available_fmv_pct,

        -- WoW inventory change and % change (vs prior week)
        (m.current_available_fmv - m.prior_week_available_fmv) AS wow_change_available_fmv,
        CASE
            WHEN m.prior_week_available_fmv IS NULL
                 OR m.prior_week_available_fmv = 0 THEN NULL
            ELSE (m.current_available_fmv - m.prior_week_available_fmv)
                 / m.prior_week_available_fmv
        END AS wow_change_available_fmv_pct,

        -- inventory per current active store in channel
        CASE
            WHEN m.current_store_count IS NULL OR m.current_store_count = 0
                 THEN NULL
            ELSE m.current_available_fmv / m.current_store_count
        END AS current_inv_per_store,

        -- rank channels by current available FMV
        ROW_NUMBER() OVER (
            ORDER BY m.current_available_fmv DESC
        ) AS channel_rank_by_current_inv
    FROM merged_channel_metrics m
)

-- FINAL SELECT
SELECT
    channel,
    WJXBFS1,  -- current available FMV
    WJXBFS2,  -- last-year available FMV (same date)
    WJXBFS3,  -- current in-transit FMV
    WJXBFS4,  -- prior-week available FMV
    WJXBFS5,  -- current store count
    WJXBFS6,  -- last-year store count

    total_curr_avail_all_channels,
    curr_avail_share_of_total,
    yoy_change_available_fmv,
    yoy_change_available_fmv_pct,
    wow_change_available_fmv,
    wow_change_available_fmv_pct,
    current_inv_per_store,
    channel_rank_by_current_inv
FROM final_with_windows
ORDER BY
    channel_rank_by_current_inv;
