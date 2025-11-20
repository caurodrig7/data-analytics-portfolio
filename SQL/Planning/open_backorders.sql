/* 
--------------------------------------------------------------------------------
SQL Script: Open Backorder – SKU Level with On Hand, On Order & Next PO
--------------------------------------------------------------------------------
Objective:
    Build a complete SKU-level backorder visibility dataset combining open
    backorders, DC on-hand inventory, open purchase orders, next-arriving POs,
    pricing information, receipt aging, merchandising hierarchy, and vendor
    attributes. Provide supply-chain planning teams with a unified view of
    oversold risk, coverage ratios, inventory shortages, and PO timing.

Definition:
    - Open Backorder:
        • Any sales_line with is_backorder = 1 AND status = 'open'
        • Aggregated at SKU level across all locations
        • Includes earliest backorder date, total BO units, total BO dollars
        • Backorder age = days since earliest BO date

    - DC Inventory:
        • Daily snapshot from product_history for DCs 903, 904, and location 2
        • Includes on-hand units per DC
        • Tied to inventory_snapshot_date parameter

    - Open Purchase Orders (DC 904):
        • Total open PO units at DC 904 from po_details
        • Negative open units are zero-capped

    - Next Arriving POs:
        • Up to the next 3 POs ranked by arrival_date
        • “Next PO” = RN = 1 (earliest upcoming PO)
        • Includes ETA date, PO number, and open units for DC 904 only

    - Pricing & Receipt Aging:
        • Latest retail price derived from product_price_history (window rank)
        • Price type logic derived from cents digit & ending code
        • Receipt age bucket classification (13, 26, 39, 52 weeks)

    - Merchandising / Vendor Dimensions:
        • Level-3 taxonomy, department name/code
        • Product & vendor name attributes

    - Derived Metrics:
        • Oversold units = Backorder units – Next PO units (floored at 0)
        • Coverage ratio vs DC 904 (OH + Open PO) / Backorders
        • BO-to-total-DC-inventory ratio
        • Window-based ranking of BO severity within department

Scope:
    - Includes ALL SKUs with:
        • Any open backorder activity OR
        • Any DC inventory OR
        • Any open PO OR
        • Any pricing or receipt history
    - No filtering by vendor, product, department, or channel
    - Date filter applied only to BO eligibility and inventory snapshot
    - Supports allocation, replenishment, supply-chain, and financial planning

Processing Steps:
    1. Load BO, inventory-snapshot, and PO parameters.
    2. Build backorders by line, then aggregate to SKU-level.
    3. Pull DC on-hand inventory for the snapshot date.
    4. Aggregate open POs at DC 904.
    5. Rank and extract next 3 POs, filter to "Next PO" for DC 904.
    6. Determine latest price and price type via window functions.
    7. Join product/vendor/taxonomy dimensions.
    8. Assemble unified SKU-level fact table combining:
         • BO metrics
         • DC OH
         • Open POs
         • Next PO details
         • Pricing & aging
         • Merchandising info
    9. Add window analytics:
         • Total BO per department
         • BO share within department
         • BO rank within department
    10. Apply reporting filter for earliest BO date threshold.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH
-- 0) Parameter CTEs
params AS (
    SELECT
        DATE('2025-11-18') AS inventory_snapshot_date,
        DATE('2023-01-01') AS min_backorder_order_date,
        DATE('2023-02-01') AS min_earliest_backorder_date_for_reporting
),

-- 1) Backorder lines by product - Current Open Backorders at line level → aggregated at product level
backorder_lines_raw AS (
    SELECT
        sl.product_analytics_id,
        sl.location_analytics_id,
        sl.sales_channel,
        sl.order_line_analytics_id,
        MIN(DATE(sl.date_ordered))       AS earliest_order_date,
        SUM(sl.quantity)                 AS current_bo_units,
        SUM(sl.merchandise)              AS current_bo_dollars
    FROM peep.sales_line sl
    JOIN params p
      ON DATE(sl.date_ordered) >= p.min_backorder_order_date
    WHERE sl.is_backorder = 1
      AND sl.status = 'open'
      AND sl.product_analytics_id IS NOT NULL
    GROUP BY
        sl.product_analytics_id,
        sl.location_analytics_id,
        sl.sales_channel,
        sl.order_line_analytics_id
),
backorder_by_sku AS (
    SELECT
        br.product_analytics_id,
        MIN(br.earliest_order_date)      AS earliest_backorder_date,
        SUM(br.current_bo_dollars)       AS open_backorder_dollars,
        SUM(br.current_bo_units)         AS open_backorder_units,
        -- extra derived: age in days from earliest BO date
        DATEDIFF(CURRENT_DATE(), MIN(br.earliest_order_date)) AS earliest_backorder_age_days
    FROM backorder_lines_raw br
    GROUP BY
        br.product_analytics_id
),

-- 2) DC On Hand inventory by product - DC 904, 903, and 2 inventory snapshot on a given day
dc_inventory AS (
    SELECT
        ph.product_analytics_id,
        SUM(CASE WHEN ph.location_analytics_id = 904 THEN ph.quantity_on_hand ELSE 0 END) AS dc_904_on_hand_units,
        SUM(CASE WHEN ph.location_analytics_id = 903 THEN ph.quantity_on_hand ELSE 0 END) AS dc_903_on_hand_units,
        SUM(CASE WHEN ph.location_analytics_id = 2   THEN ph.quantity_on_hand ELSE 0 END) AS dc_2_on_hand_units
    FROM product_history ph
    JOIN calendar c
      ON ph.date_analytics_id = c.date_analytics_id
    JOIN params p
      ON DATE(c.gregorian_date) = p.inventory_snapshot_date
    WHERE ph.location_analytics_id IN (904, 903, 2)
    GROUP BY
        ph.product_analytics_id
),

-- 3) Open PO quantity at DC 904 - All open order units at DC 904
dc_904_open_po AS (
    SELECT
        pod.product_analytics_id,
        SUM(
            CASE
                WHEN pod.quantity_open < 0 THEN 0
                ELSE pod.quantity_open
            END
        ) AS dc_904_open_on_order_units
    FROM peep.po_details pod
    WHERE pod.is_closed = 0
      AND pod.location_analytics_id = 904
      AND pod.product_analytics_id IS NOT NULL
    GROUP BY
        pod.product_analytics_id
),

-- 4) Next 3 POs per product, filtered to DC 904, then only the “Next PO”
mpo AS (
    SELECT
        pod.product_analytics_id,
        pod.location_analytics_id,
        pod.current_arrival_date_analytics_id,
        DATE(pod.current_arrival_date)   AS current_arrival_date,
        pod.purchase_order_analytics_id,
        pod.sku,
        CASE
            WHEN pod.quantity_open < 0 THEN 0
            ELSE pod.quantity_open
        END                              AS quantity_open,
        ROW_NUMBER() OVER (
            PARTITION BY pod.product_analytics_id
            ORDER BY pod.current_arrival_date_analytics_id
        ) AS rn
    FROM peep.po_details pod
    WHERE pod.quantity_open > 0
      AND pod.product_analytics_id IS NOT NULL
      AND pod.is_closed = 0
),
next_po_ranked AS (
    SELECT
        m.product_analytics_id,
        m.location_analytics_id,
        m.rn AS next_po_order,
        CASE
            WHEN m.rn = 1 THEN 'Next PO'
            WHEN m.rn = 2 THEN '2nd Next PO'
            WHEN m.rn = 3 THEN '3rd Next PO'
            ELSE CONCAT(m.rn, 'th Next PO')
        END AS next_po_order_desc,
        m.current_arrival_date_analytics_id,
        m.current_arrival_date,
        m.purchase_order_analytics_id,
        m.sku,
        m.quantity_open
    FROM mpo m
    WHERE m.rn < 4
),
next_po_904 AS (
    SELECT
        npr.product_analytics_id,
        SUM(npr.quantity_open)          AS next_po_units_904,
        MAX(npr.current_arrival_date)   AS next_po_eta_904,
        MAX(npr.purchase_order_analytics_id) AS next_po_number_904
    FROM next_po_ranked npr
    WHERE npr.location_analytics_id = 904
      AND npr.next_po_order = 1
    GROUP BY
        npr.product_analytics_id
),

-- 5) Latest price + price type & receipt age
latest_price AS (
    SELECT
        pph.sku,
        pph.product_analytics_id,
        pph.price,
        RANK() OVER (
            PARTITION BY pph.product_analytics_id
            ORDER BY pph.snapshot_date DESC
        ) AS r1
    FROM peep.product_price_history pph
),
price_info AS (
    SELECT
        p.product_analytics_id,
        p.retail_price AS web_price,
        CASE
            WHEN lp.price IS NULL THEN p.retail_price
            ELSE lp.price
        END AS current_retail_price,

        p.last_receipt_date,
        DATEDIFF(CURRENT_DATE(), p.last_receipt_date) AS last_receipt_age,

        -- Price Type ID / Code / Description
        CASE
            WHEN (FLOOR(CASE WHEN lp.price IS NULL THEN p.retail_price ELSE lp.price END * 100) % 10) = 6 THEN 3
            WHEN (FLOOR(CASE WHEN lp.price IS NULL THEN p.retail_price ELSE lp.price END * 100) % 10) = 9 THEN 2
            WHEN RIGHT(CAST(
                    CASE WHEN lp.price IS NULL THEN p.retail_price ELSE lp.price END
                    AS CHAR(10)
                 ), 2) = '01' THEN 4
            WHEN (CASE WHEN lp.price IS NULL THEN p.retail_price ELSE lp.price END) IS NULL THEN 5
            ELSE 1
        END AS current_price_type_id,

        CASE
            WHEN (FLOOR(CASE WHEN lp.price IS NULL THEN p.retail_price ELSE lp.price END * 100) % 10) = 6 THEN 'POS'
            WHEN (FLOOR(CASE WHEN lp.price IS NULL THEN p.retail_price ELSE lp.price END * 100) % 10) = 9 THEN 'MKD'
            WHEN RIGHT(CAST(
                    CASE WHEN lp.price IS NULL THEN p.retail_price ELSE lp.price END
                    AS CHAR(10)
                 ), 2) = '01' THEN 'MOS'
            WHEN (CASE WHEN lp.price IS NULL THEN p.retail_price ELSE lp.price END) IS NULL THEN 'NP'
            ELSE 'REG'
        END AS current_price_type_code,

        CASE
            WHEN (FLOOR(CASE WHEN lp.price IS NULL THEN p.retail_price ELSE lp.price END * 100) % 10) = 6 THEN 'POS'
            WHEN (FLOOR(CASE WHEN lp.price IS NULL THEN p.retail_price ELSE lp.price END * 100) % 10) = 9 THEN 'Markdown'
            WHEN RIGHT(CAST(
                    CASE WHEN lp.price IS NULL THEN p.retail_price ELSE lp.price END
                    AS CHAR(10)
                 ), 2) = '01' THEN 'MOS'
            WHEN (CASE WHEN lp.price IS NULL THEN p.retail_price ELSE lp.price END) IS NULL THEN 'No Price'
            ELSE 'Regular'
        END AS current_price_type_description,

        -- Receipt age buckets
        CASE
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) <= 91 THEN 5
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) BETWEEN 92 AND 182 THEN 4
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) BETWEEN 183 AND 273 THEN 3
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) BETWEEN 274 AND 364 THEN 2
            ELSE 1
        END AS receipt_age_bucket_id,

        CASE
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) <= 91 THEN 'Aged Last 13 Weeks'
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) BETWEEN 92 AND 182 THEN 'Aged 14 - 26 Weeks'
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) BETWEEN 183 AND 273 THEN 'Aged 27 - 39 Weeks'
            WHEN DATEDIFF(CURRENT_DATE(), p.last_receipt_date) BETWEEN 274 AND 364 THEN 'Aged 40 - 52 Weeks'
            ELSE 'Aged Greater than 52 Weeks'
        END AS receipt_age_bucket
    FROM peep.products p
    LEFT JOIN (
        SELECT
            sku,
            product_analytics_id,
            price
        FROM latest_price
        WHERE r1 = 1
    ) lp
      ON p.product_analytics_id = lp.product_analytics_id
),

-- 6) Department dimension 
department_dim AS (
    SELECT
        taxonomy_analytics_id        AS department_id,
        parent_taxonomy_analytics_id AS division_id,
        name                         AS department_name,
        CAST(taxonomy_code AS SIGNED) AS department_code
    FROM peep.merchandising_taxonomies
    WHERE level = 3
),

-- 7) Base dimension join (product + vendor + taxonomy)
product_dim AS (
    SELECT
        p.product_analytics_id,
        p.product_name,
        COALESCE(p.vendor_analytics_id, 0)  AS vendor_analytics_id,
        COALESCE(v.vendor_name, 'Blank')    AS vendor_name,
        pmt.level_3_analytics_id,
        dd.department_name,
        dd.department_code
    FROM peep.products p
    LEFT JOIN vendors v
      ON p.vendor_analytics_id = v.vendor_analytics_id
    JOIN product_to_merchandising_taxonomy pmt
      ON p.product_analytics_id = pmt.product_analytics_id
    JOIN department_dim dd
      ON pmt.level_3_analytics_id = dd.department_id
),

-- 8) Full SKU-level fact assembly (joining all measures)
open_bo_sku_fact AS (
    SELECT
        pd.level_3_analytics_id,
        pd.department_name  AS level_3_name,
        pd.department_code  AS taxonomy_code,
        pd.product_analytics_id,
        pd.product_name,
        pd.vendor_analytics_id,
        pd.vendor_name,
        pi.current_retail_price         AS file_retail_price,

        -- Backorder measures
        bo.earliest_backorder_date,
        bo.open_backorder_dollars,
        bo.open_backorder_units,

        -- DC inventory
        inv.dc_904_on_hand_units,
        inv.dc_903_on_hand_units,
        inv.dc_2_on_hand_units,

        -- DC 904 Open PO
        po_open.dc_904_open_on_order_units,

        -- Next PO @ DC 904
        next_po.next_po_units_904,
        next_po.next_po_eta_904,
        next_po.next_po_number_904,

        -- Derived: oversold units (BO units - next PO units, floored at 0)
        GREATEST(
            (COALESCE(bo.open_backorder_units, 0)
             - COALESCE(next_po.next_po_units_904, 0)),
            0
        ) AS oversold_backorder_units,

        -- Extra: coverage ratio vs DC 904 OH + Open PO
        CASE
            WHEN COALESCE(bo.open_backorder_units, 0) = 0 THEN NULL
            ELSE (COALESCE(inv.dc_904_on_hand_units, 0)
                  + COALESCE(po_open.dc_904_open_on_order_units, 0))
                 / COALESCE(bo.open_backorder_units, 0)
        END AS bo_coverage_ratio_904,

        -- Extra: backorder to total DC inventory ratio
        CASE
            WHEN (COALESCE(inv.dc_904_on_hand_units, 0)
                  + COALESCE(inv.dc_903_on_hand_units, 0)
                  + COALESCE(inv.dc_2_on_hand_units, 0)) = 0
            THEN NULL
            ELSE COALESCE(bo.open_backorder_units, 0)
                / (COALESCE(inv.dc_904_on_hand_units, 0)
                   + COALESCE(inv.dc_903_on_hand_units, 0)
                   + COALESCE(inv.dc_2_on_hand_units, 0))
        END AS bo_to_total_dc_inventory_ratio,

        -- Carry along receipt-age and price-type info
        pi.last_receipt_date,
        pi.last_receipt_age,
        pi.current_price_type_id,
        pi.current_price_type_code,
        pi.current_price_type_description,
        pi.receipt_age_bucket_id,
        pi.receipt_age_bucket
    FROM product_dim pd
    LEFT JOIN backorder_by_sku bo
      ON pd.product_analytics_id = bo.product_analytics_id
    LEFT JOIN dc_inventory inv
      ON pd.product_analytics_id = inv.product_analytics_id
    LEFT JOIN dc_904_open_po po_open
      ON pd.product_analytics_id = po_open.product_analytics_id
    LEFT JOIN next_po_904 next_po
      ON pd.product_analytics_id = next_po.product_analytics_id
    LEFT JOIN price_info pi
      ON pd.product_analytics_id = pi.product_analytics_id
),

-- 9) Add window metrics for ranking & share
open_bo_sku_with_windows AS (
    SELECT
        f.*,

        -- Total open backorder units per department
        SUM(COALESCE(f.open_backorder_units, 0)) OVER (
            PARTITION BY f.level_3_analytics_id
        ) AS total_bo_units_in_dept,

        -- Share of BO units within department
        CASE
            WHEN SUM(COALESCE(f.open_backorder_units, 0)) OVER (
                    PARTITION BY f.level_3_analytics_id
                 ) = 0
            THEN NULL
            ELSE COALESCE(f.open_backorder_units, 0)
                 / SUM(COALESCE(f.open_backorder_units, 0)) OVER (
                     PARTITION BY f.level_3_analytics_id
                 )
        END AS bo_unit_share_in_dept,

        -- Rank SKUs within department by backorder dollars
        ROW_NUMBER() OVER (
            PARTITION BY f.level_3_analytics_id
            ORDER BY COALESCE(f.open_backorder_dollars, 0) DESC,
                     COALESCE(f.open_backorder_units, 0) DESC,
                     f.product_analytics_id
        ) AS bo_rank_in_dept
    FROM open_bo_sku_fact f
)

-- FINAL SELECT
SELECT
    level_3_analytics_id,
    level_3_name,
    taxonomy_code,
    product_analytics_id,
    product_name,
    vendor_analytics_id,
    vendor_name,
    file_retail_price,

    earliest_backorder_date,
    open_backorder_dollars,
    open_backorder_units,

    dc_904_on_hand_units,
    dc_903_on_hand_units,
    dc_2_on_hand_units,

    dc_904_open_on_order_units,
    next_po_units_904,
    next_po_eta_904,
    next_po_number_904,
    oversold_backorder_units,
    bo_coverage_ratio_904,
    bo_to_total_dc_inventory_ratio,

    last_receipt_date,
    last_receipt_age,
    current_price_type_id,
    current_price_type_code,
    current_price_type_description,
    receipt_age_bucket_id,
    receipt_age_bucket,

    total_bo_units_in_dept,
    bo_unit_share_in_dept,
    bo_rank_in_dept
FROM open_bo_sku_with_windows
JOIN params p
  ON (earliest_backorder_date IS NULL
      OR earliest_backorder_date >= p.min_earliest_backorder_date_for_reporting)
ORDER BY
    level_3_analytics_id,
    bo_rank_in_dept,
    product_analytics_id;
