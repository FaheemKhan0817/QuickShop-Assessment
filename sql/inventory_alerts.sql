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


"""
# Results:

product_id	product_name	category	stock_on_hand
1025	Product_1025	Apparel       	56
1016	Product_1016	Footwear	    77
1015	Product_1015	Accessories	    94
1014	Product_1014	Apparel	        99


"""