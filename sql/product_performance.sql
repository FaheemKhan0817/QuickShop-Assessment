-- Tested on MySQL 8.0+
-- Data loaded using MySQL Workbench CSV Import Wizard

-- This query shows which products are performing best.
-- It calculates total units sold, total revenue, and return rate for each product.

SELECT
    p.product_id,                       -- product id
    p.product_name,                     -- name of the product
    p.category,                         -- product category
    SUM(o.qty) AS total_units_sold,     -- total number of items sold
    SUM(o.qty * o.unit_price) AS total_revenue,   -- total money made from that product
    ROUND(
        SUM(CASE WHEN o.order_status='cancelled' THEN 1 ELSE 0 END)/COUNT(*),
        2
    ) AS return_rate                    -- how many orders got cancelled (return rate)
FROM orders o
JOIN products p ON o.product_id = p.product_id     -- matching orders with product info
GROUP BY
    p.product_id, p.product_name, p.category       -- group data by each product
ORDER BY
    total_revenue DESC;                -- show the highest-earning products first

