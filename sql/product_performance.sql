-- Tested on MySQL 8.0+
-- Data loaded using MySQL Workbench CSV Import Wizard
-- product_performance.sql
-- Calculate product-level metrics: total units sold, total revenue, and return rate (if available)
-- Using LEFT JOIN to include products with zero sales

SELECT
    p.product_id,
    p.product_name,
    p.category,
    COALESCE(SUM(o.qty), 0) AS total_units_sold,                       -- Total quantity sold per product
    COALESCE(SUM(o.qty * o.unit_price), 0) AS total_revenue,           -- Total revenue per product
    0.00 AS return_rate                                                -- NOTE: return_rate currently a placeholder (0.00). Replace with real returns data if available.

FROM products p
LEFT JOIN orders o
       ON p.product_id = o.product_id
      AND o.order_status = 'completed'
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC;
