-- Tested on MySQL 8.0+
-- Data loaded into MySQL via CSV import (MySQL Workbench CSV Import Wizard).
-- cohort_retention.sql
-- This query calculates cohort retention by user first-order month.
-- Note: For meaningful multi-month retention curves, run against dataset covering multiple months.


WITH first_orders AS (
    -- Identify the first month each user placed an order
    SELECT
        user_id,
        DATE_FORMAT(MIN(order_date), '%Y-%m') AS cohort_month
    FROM orders
    GROUP BY user_id
),
orders_with_cohort AS (
    -- Associate each order with the user's cohort month
    SELECT
        o.user_id,
        f.cohort_month,
        DATE_FORMAT(o.order_date, '%Y-%m') AS order_month
    FROM orders o
    JOIN first_orders f ON o.user_id = f.user_id
),
cohort_counts AS (
    -- Count distinct users for each cohort in each month
    SELECT
        cohort_month,
        order_month,
        COUNT(DISTINCT user_id) AS user_count
    FROM orders_with_cohort
    GROUP BY cohort_month, order_month
),
cohort_sizes AS (
    -- Determine total users in each cohort
    SELECT
        cohort_month,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM first_orders
    GROUP BY cohort_month
)
-- Calculate retention rate per cohort per month
SELECT
    c.cohort_month,
    c.order_month,
    c.user_count,
    cs.cohort_size,
    ROUND(c.user_count / cs.cohort_size, 2) AS retention_rate
FROM cohort_counts c
JOIN cohort_sizes cs ON c.cohort_month = cs.cohort_month
ORDER BY c.cohort_month, c.order_month;  -- show in chronological order
