-- Tested on MySQL 8.0+
-- Data loaded using MySQL Workbench CSV Import Wizard
-- inventory_alerts.sql
-- Identify products with stock below a threshold (e.g., 100 units)

SELECT
    i.product_id,
    p.product_name,
    p.category,
    i.stock_on_hand
FROM inventory i
JOIN products p ON i.product_id = p.product_id
WHERE i.stock_on_hand < 100
ORDER BY i.stock_on_hand ASC;
