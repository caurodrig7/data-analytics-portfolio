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

WITH recent_orders AS (
    SELECT 
        order_number,
        ecom_order_number,
        alt_order_number_2,
        date_ordered,
        ROW_NUMBER() OVER (PARTITION BY alt_order_number_2 ORDER BY date_ordered DESC) AS rn
    FROM analytics.sales_header
    WHERE 
        alt_order_number_2 IS NOT NULL
        AND alt_order_number_2 NOT LIKE '%ginating Store: 90%'
        AND DATE(date_ordered) = CURRENT_DATE - INTERVAL '1' DAY
),
cleaned_orders AS (
    SELECT
        order_number AS oroms_order_number,
        ecom_order_number AS ca_order_number,
        alt_order_number_2 AS amz_order_number,
        CAST(date_ordered AS DATE) AS order_date,
        rn
    FROM recent_orders
    WHERE rn = 1  -- latest record per Amazon order reference
)
SELECT 
    oroms_order_number,
    ca_order_number,
    amz_order_number,
    order_date
FROM cleaned_orders
ORDER BY order_date DESC, amz_order_number;
