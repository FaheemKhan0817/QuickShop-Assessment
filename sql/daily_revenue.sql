-- daily_revenue.sql
-- Calculate daily revenue and identify the top revenue category per day
-- Use window function RANK() to rank categories per day by revenue

WITH daily_category_revenue AS (
    SELECT
        DATE(o.order_date) AS order_day,
        p.category,
        SUM(o.qty * o.unit_price) AS revenue
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    WHERE o.order_status = 'completed'
    GROUP BY DATE(o.order_date), p.category
),
ranked_categories AS (
    SELECT
        order_day,
        category,
        revenue,
        RANK() OVER (PARTITION BY order_day ORDER BY revenue DESC) AS rnk  -- alias to avoid reserved keyword
    FROM daily_category_revenue
)
SELECT
    dcr.order_day,
    dcr.category AS top_category,
    dcr.revenue AS top_category_revenue,
    (
        SELECT SUM(o2.qty * o2.unit_price)
        FROM orders o2
        WHERE DATE(o2.order_date) = dcr.order_day
          AND o2.order_status = 'completed'
    ) AS total_daily_revenue
FROM ranked_categories dcr
WHERE dcr.rnk = 1   -- use the alias here
ORDER BY dcr.order_day;


"""
# Results:
order_day	top_category	top_category_revenue	total_daily_revenue
10/23/2025	Accessories	     1709.84	                      2353.82
10/24/2025	Electronics	     1459.84	                      2068.9
10/25/2025	Electronics	     3571	                      5316.67
10/26/2025	Home	         1117.24	                      3780.27


"""