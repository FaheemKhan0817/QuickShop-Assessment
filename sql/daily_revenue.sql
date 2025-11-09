-- This query finds total daily revenue and shows which product category earned the most each day.
-- Tested on MySQL 8.0+
-- Data imported using MySQL Workbench CSV Import Wizard.

-- Step 1: Calculate revenue per day for each category
WITH daily_category_revenue AS (
    SELECT
        DATE(o.order_date) AS order_day,          -- take only the date part from order_date
        p.category,                               -- product category
        SUM(o.qty * o.unit_price) AS revenue      -- total revenue for that category on that day
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    WHERE o.order_status = 'completed'            -- include only completed orders
    GROUP BY DATE(o.order_date), p.category
),

-- Step 2: Rank categories by revenue for each day
ranked_categories AS (
    SELECT
        order_day,
        category,
        revenue,
        RANK() OVER (PARTITION BY order_day ORDER BY revenue DESC) AS rnk  -- rank by revenue
    FROM daily_category_revenue
)

-- Step 3: Show only the top category per day, along with total daily revenue
SELECT
    dcr.order_day,
    dcr.category AS top_category,                 -- category with highest revenue
    dcr.revenue AS top_category_revenue,          -- revenue of that category
    (
        SELECT SUM(o2.qty * o2.unit_price)
        FROM orders o2
        WHERE DATE(o2.order_date) = dcr.order_day
          AND o2.order_status = 'completed'
    ) AS total_daily_revenue                      -- total revenue for the day
FROM ranked_categories dcr
WHERE dcr.rnk = 1                                 -- keep only the top-ranked category
ORDER BY dcr.order_day;                           -- show results by day
