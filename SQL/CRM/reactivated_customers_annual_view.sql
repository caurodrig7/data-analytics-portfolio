/*
--------------------------------------------------------------------------------
SQL Script: Reactivated Customers – Annual View
--------------------------------------------------------------------------------
Objective:
    Report reactivated customers based on delivered sales,
    summarized at the annual level with no channel duplication.

Definition:
    - Reactivated Customers: Returned after at least one fiscal year of inactivity. 

Scope:
    - Excludes Amazon channel transactions.
    - Excludes non-merchandise sales such as Gift Cards and Warranties.

Author: César Rodríguez
--------------------------------------------------------------------------------
*/

WITH yearly_customers AS (
    -- Get distinct customers and the years they made purchases
    SELECT DISTINCT peep.sales_header.email, 
    YEAR(peep.sales_header.date_ordered) AS order_year
    FROM peep.delivered_sales
	LEFT JOIN peep.sales_header
		ON peep.sales_header.order_analytics_id = peep.delivered_sales.order_analytics_id  
	LEFT JOIN peep.sales_line
		ON ((peep.sales_line.order_analytics_id = peep.delivered_sales.order_analytics_id) AND (peep.sales_line.order_line_analytics_id = peep.delivered_sales.order_line_analytics_id))
	WHERE peep.sales_header.email IS NOT NULL
		AND (
				(peep.sales_line.'source' = 'xcenter' AND 
					(peep.sales_line.order_line_type = 'in_store_sale')
				) OR
				(peep.sales_line.'source' = 'oroms' AND 
    				( 
         				(peep.sales_line.sales_channel = 'web' AND peep.sales_line.order_line_type = 'ecommerce') OR 
         				(peep.sales_line.sales_channel = 'culinary_orders' AND peep.sales_line.order_line_type = 'ecommerce') OR
         				(peep.sales_line.sales_channel = 'slt_bopis' AND peep.sales_line.order_line_type = 'bopis') OR 
         				(peep.sales_line.sales_channel = 'walmart_go_local' AND peep.sales_line.order_line_type = 'bopis') OR
         				(peep.sales_line.sales_channel = 'customer_service' AND peep.sales_line.order_line_type = 'phone') OR
         				(peep.sales_line.sales_channel = 'culinary_orders' AND peep.sales_line.order_line_type = 'phone')
    				)
				) 
			)
),
reactivated_customers AS (
    -- Find customers who purchased in year X, did NOT purchase in year X-1, but purchased before year X-1
    SELECT 
        c1.order_year AS year, 
        COUNT(DISTINCT c1.email) AS reactivated_customers
    FROM yearly_customers c1
    LEFT JOIN yearly_customers c2 
        ON c1.email = c2.email 
        AND c1.order_year = c2.order_year + 1  -- Check if the customer purchased in year X-1
    JOIN yearly_customers c3 
        ON c1.email = c3.email 
        AND c3.order_year < c1.order_year - 1  -- Check if the customer purchased before X-1
    WHERE c2.email IS NULL  -- Ensure the customer did NOT purchase in year X-1
    GROUP BY c1.order_year
)
SELECT * 
FROM reactivated_customers
ORDER BY year;