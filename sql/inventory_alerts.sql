-- Tested on MySQL 8.0+
-- Data loaded using MySQL Workbench CSV Import Wizard
-- inventory_alerts.sql
-- Identify products with stock below a threshold (e.g., 100 units)

SELECT
    i.product_id,           -- product id
    p.product_name,         -- name of the product
    p.category,             -- product category
    i.stock_on_hand         -- available stock
FROM inventory i
JOIN products p ON i.product_id = p.product_id    -- join to get product details
WHERE i.stock_on_hand < 100                       -- filter products with low stock
ORDER BY i.stock_on_hand ASC;                     -- show lowest stock first

