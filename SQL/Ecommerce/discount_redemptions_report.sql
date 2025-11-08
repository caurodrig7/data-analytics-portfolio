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

select sfcc.sfcc_discount.discount_type
, sfcc.sfcc_discount.campaign_name
, sfcc.sfcc_discount.promotion_name
, sfcc.sfcc_discount.discount_name
, count(sfcc.sfcc_discount.total) as total_redemptions
, sum(sfcc.sfcc_discount.total) as total_discount
from sfcc.sfcc_discount
left join sfcc.sfcc_order
  on sfcc.sfcc_order.sfcc_order_number = sfcc.sfcc_discount.sfcc_order_number
where date(sfcc.sfcc_order.order_date) = CURRENT_DATE-1
group by sfcc.sfcc_discount.discount_type
, sfcc.sfcc_discount.campaign_name
, sfcc.sfcc_discount.promotion_name
, sfcc.sfcc_discount.discount_name
order by sfcc.sfcc_discount.discount_type
, sfcc.sfcc_discount.campaign_name
, sfcc.sfcc_discount.promotion_name
, sfcc.sfcc_discount.discount_name;