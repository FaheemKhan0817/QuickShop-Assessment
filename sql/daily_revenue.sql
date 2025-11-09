-- Purpose:
--   This query calculates the total daily revenue and identifies the
--   top-performing product category for each day based on sales revenue.
--
-- Environment:
--   ✅ Tested on MySQL 8.0+
--   ✅ Data imported using MySQL Workbench CSV Import Wizard
--      (Tables: orders, products)

-- Step 1: Aggregate revenue per day per category
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

-- Step 2: Rank categories within each day by revenue
ranked_categories AS (
    SELECT
        order_day,
        category,
        revenue,
        RANK() OVER (
            PARTITION BY order_day
            ORDER BY revenue DESC
        ) AS rnk  -- use alias to avoid reserved keyword issues
)

-- Step 3: Pick only the top category (rank = 1) for each day
SELECT
    dcr.order_day,
    dcr.category AS top_category,
    dcr.revenue AS top_category_revenue,

    -- Step 4: Compute total revenue for that day (all categories)
    (
        SELECT SUM(o2.qty * o2.unit_price)
        FROM orders o2
        WHERE DATE(o2.order_date) = dcr.order_day
          AND o2.order_status = 'completed'
    ) AS total_daily_revenue

FROM ranked_categories dcr
WHERE dcr.rnk = 1
ORDER BY dcr.order_day;
