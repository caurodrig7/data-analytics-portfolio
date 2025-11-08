/* 
--------------------------------------------------------------------------------
SQL Script: Daily Discount Redemptions Report
--------------------------------------------------------------------------------
Objective:
    Summarize daily discount activity across all promotions to measure 
    redemption volume and total discount value applied to customer orders.

Definition:
    - Redemption:
        • Occurs when a discount, coupon, or campaign promotion is applied 
          to an order in Salesforce (SFCC).
    - Metrics:
        • total_redemptions: Count of discount instances applied.
        • total_discount: Sum of discount value across all redeemed orders.
    - Grouping:
        • Aggregated by discount_type, campaign_name, promotion_name, 
          and discount_name.

Scope:
    - Includes orders placed on the previous calendar day (CURRENT_DATE - 1).

Author: Cesar Rodriguez
--------------------------------------------------------------------------------
*/

WITH daily_discounts AS (
    SELECT
        d.discount_type,
        d.campaign_name,
        d.promotion_name,
        d.discount_name,
        d.total AS discount_amount,
        o.order_date
    FROM sfcc.sfcc_discount AS d
    LEFT JOIN sfcc.sfcc_order AS o
        ON o.sfcc_order_number = d.sfcc_order_number
    WHERE CAST(o.order_date AS DATE) = CURRENT_DATE - INTERVAL '1' DAY
),
aggregated_discounts AS (
    SELECT
        discount_type,
        campaign_name,
        promotion_name,
        discount_name,
        COUNT(*) AS total_redemptions,
        SUM(discount_amount) AS total_discount
    FROM daily_discounts
    GROUP BY
        discount_type,
        campaign_name,
        promotion_name,
        discount_name
),
ranked_discounts AS (
    SELECT
        ad.*,
        SUM(ad.total_discount) OVER (
            PARTITION BY ad.campaign_name
        ) AS campaign_total_discount,
        CASE 
            WHEN SUM(ad.total_discount) OVER (PARTITION BY ad.campaign_name) = 0 
                THEN 0
            ELSE 
                ROUND(
                    100.0 * ad.total_discount 
                    / SUM(ad.total_discount) OVER (PARTITION BY ad.campaign_name),
                    2
                )
        END AS pct_of_campaign_discount,
        RANK() OVER (
            PARTITION BY ad.campaign_name
            ORDER BY ad.total_discount DESC
        ) AS promotion_rank_in_campaign
    FROM aggregated_discounts AS ad
)

SELECT
    discount_type,
    campaign_name,
    promotion_name,
    discount_name,
    total_redemptions,
    total_discount,
    campaign_total_discount,
    pct_of_campaign_discount,
    promotion_rank_in_campaign
FROM ranked_discounts
ORDER BY
    discount_type,
    campaign_name,
    promotion_rank_in_campaign,
    promotion_name,
    discount_name;
