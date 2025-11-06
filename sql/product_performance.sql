-- product_performance.sql
-- Calculate product-level metrics: total units sold, total revenue, and return rate (if available)
-- Using LEFT JOIN to include products with zero sales

SELECT
    p.product_id,
    p.product_name,
    p.category,
    COALESCE(SUM(o.qty), 0) AS total_units_sold,                       -- Total quantity sold per product
    COALESCE(SUM(o.qty * o.unit_price), 0) AS total_revenue,           -- Total revenue per product
    0.00 AS return_rate                                                 -- Placeholder: adjust if return data exists
FROM products p
LEFT JOIN orders o
       ON p.product_id = o.product_id
      AND o.order_status = 'completed'
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC;

"""
# Results:

product_id	product_name	category	total_units_sold	total_revenue	return_rate
1004	Product_1004	Electronics	6	2959.8	0
1022	Product_1022	Footwear	7	1740.69	0
1009	Product_1009	Accessories	4	1470	0
1021	Product_1021	Electronics	6	1325.4	0
1005	Product_1005	Home	4	1117.24	0
1010	Product_1010	Electronics	2	966.54	0
1007	Product_1007	Accessories	2	862.24	0
1025	Product_1025	Apparel	5	630.95	0
1020	Product_1020	Home	3	515.16	0
1014	Product_1014	Apparel	5	496.95	0
1018	Product_1018	Electronics	3	465.42	0
1029	Product_1029	Footwear	3	286.62	0
1024	Product_1024	Apparel	1	265.41	0
1015	Product_1015	Accessories	1	154.4	0
1026	Product_1026	Footwear	8	150.24	0
1006	Product_1006	Accessories	4	112.6	0
"""