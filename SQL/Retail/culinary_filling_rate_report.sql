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
 
select  store.name store_name
  , analytics.culinary_products.location_code store
  , store.district_code district_code
  , date(analytics.culinary_products.start_date) date_start
  , cast(analytics.culinary_products.start_date as time) time_start
  , analytics.culinary_products.sku class_sku
  , products.short_description class_name
  , analytics.culinary_products.instructor_name chef_name
  , analytics.culinary_products.maximum_seats max_seats
  , sum(isnull(sales.quantity,0)) paid_in_full
  , (max_seats - paid_in_full) available_seats
  , (paid_in_full / max_seats) fill_rate
  , isnull(avg(item.unit_price),min(analytics.culinary_products.price)) cost
  , (sum(isnull(sales.sub_total,0)) + (available_seats * min(analytics.culinary_products.price))) potential
  , sum(isnull(sales.sub_total,0)) actual
  , analytics.culinary_products.is_class_cancelled is_class_cancelled
from analytics.culinary_products 
left join analytics.locations store
  on store.location_code = analytics.culinary_products.location_code
left join analytics.products products
  on products.sku = analytics.culinary_products.sku
left join analytics.sales_line sales
  on (sales.sku = analytics.culinary_products.sku) and (sales.is_return != 'TRUE' or sales.is_return is null) and (sales.is_canceled != 'TRUE' or sales.is_canceled is null)
left join analytics.sales_header header
  on header.order_number = sales.order_number
left join sfcc.sfcc_order_item item
  on (item.sfcc_order_number = header.ecom_order_number) and (item.sku=sales.sku) 
where class_sku NOT LIKE 'CSA%'
and analytics.culinary_products.start_date >= '2023-01-29 00:00:00'
and analytics.culinary_products.start_date <= '2024-02-03 23:59:59'
group by analytics.culinary_products.location_code
, store.name
, store.district_code
, analytics.culinary_products.start_date
, analytics.culinary_products.sku
, products.short_description
, analytics.culinary_products.instructor_name
, analytics.culinary_products.maximum_seats
, analytics.culinary_products.is_class_cancelled
, order by analytics.culinary_products.start_date
, analytics.culinary_products.location_code;