/* 
--------------------------------------------------------------------------------
SQL Script: Amazon Orders Daily Extract
--------------------------------------------------------------------------------
Objective:
    Identify and extract all Amazon (AMZ) orders processed through the 
    Order Management System (OROMS) on the previous day.

Definition:
    - Amazon Order:
        • Order records with a non-null Amazon order reference .
        • Excludes invalid or placeholder entries such as 
          'ginating Store: 90'.
    - Date Filter:
        • Includes only orders placed on the previous calendar day 
          (CURRENT_DATE - 1).

Scope:
    - Output includes OROMS, Commerce Cloud (CA), and Amazon order numbers 
      along with their order date.
    - Intended for daily operational checks between systems 
      (OROMS <-> Amazon <-> Commerce Cloud).

Author: Cesar Rodriguez
--------------------------------------------------------------------------------
*/

select
  order_number as OROMS_order_number
, ecom_order_number as CA_Order_number
, alt_order_number_2 as AMZ_Order_number
, date_ordered
from analytics.sales_header
where (alt_order_number_2 is not null) and (alt_order_number_2 !='ginating Store: 90')
and date(date_ordered) = CURRENT_DATE-1;