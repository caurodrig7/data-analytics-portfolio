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

select min(date(analytics.sales_line.date_ordered)) as 'Date'
, min(hour(analytics.sales_line.date_ordered)) as 'Hour'
, sum(analytics.sales_line.merchandise) as 'Demand'
from analytics.sales_line
left join analytics.sales_header
  on analytics.sales_header.order_number = analytics.sales_line.order_number
left join sfcc.sfcc_order
  on sfcc.sfcc_order.sfcc_order_number = analytics.sales_header.ecom_order_number
left join sfcc.sfcc_order_item
  on (sfcc.sfcc_order_item.sfcc_order_number = sfcc.sfcc_order.sfcc_order_number) and (sfcc.sfcc_order_item.sku = analytics.sales_line.sku)
where analytics.sales_line.is_canceled = 'FALSE'
and analytics.sales_line.is_return = 'FALSE'
and ((analytics.sales_line.order_line_type = 'ecommerce') or (analytics.sales_line.order_line_type = 'bopis'))
and analytics.sales_header."source" = 'oroms'
and sfcc.sfcc_order_item.product_type != 'Culinary'
and date(analytics.sales_line.date_ordered) >= '2022-10-02'
and date(analytics.sales_line.date_ordered) <= '2022-10-14'
group by date(analytics.sales_line.date_ordered)
, hour(analytics.sales_line.date_ordered)
order by date(analytics.sales_line.date_ordered)
, hour(analytics.sales_line.date_ordered);