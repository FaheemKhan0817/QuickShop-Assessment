# SQL Query Sample Outputs

> **Tested on:** MySQL 8.0+  
> **Data Source:** CSVs imported using *MySQL Workbench CSV Import Wizard*  
> **Dataset Period:** 2025-10-23 → 2025-10-26  
> **Note:** Values are from the processed QuickShop ETL output.  
> `return_rate` is a placeholder (0.00) because no returns data exists.

---

## 🧮 1. `cohort_retention.sql`

**Purpose:** Identify monthly user cohorts and calculate retention over time.

| cohort_month | order_month | user_count | cohort_size | retention_rate |
|---------------|--------------|-------------|--------------|----------------|
| 2025-10 | 2025-10 | 20 | 20 | 1.00 |

> All users who made their first purchase in October 2025 also placed an order in the same month (100% retention).  
> For multi-month retention, run the query on a larger date range.

---

## 💰 2. `daily_revenue.sql`

**Purpose:** Compute daily total revenue and identify the top-earning category per day.

| order_day | top_category | top_category_revenue | total_daily_revenue |
|------------|---------------|----------------------|----------------------|
| 2025-10-23 | Accessories | 1709.84 | 2353.82 |
| 2025-10-24 | Electronics | 1459.84 | 2068.90 |
| 2025-10-25 | Electronics | 3571.00 | 5316.67 |
| 2025-10-26 | Home | 1117.24 | 3780.27 |

> Uses `RANK()` window function to identify top categories.  
> If multiple categories have equal revenue, all will appear.

---

## 📦 3. `inventory_alerts.sql`

**Purpose:** Flag products with low stock (< 100 units).

| product_id | product_name | category | stock_on_hand |
|-------------|---------------|-----------|----------------|
| 1025 | Product_1025 | Apparel | 56 |
| 1016 | Product_1016 | Footwear | 77 |
| 1015 | Product_1015 | Accessories | 94 |
| 1014 | Product_1014 | Apparel | 99 |

> Helps identify products requiring restock attention.

---

## 📊 4. `product_performance.sql`

**Purpose:** Calculate product-level metrics such as total units sold, total revenue, and return rate.

| product_id | product_name | category | total_units_sold | total_revenue | return_rate |
|-------------|---------------|-----------|------------------|---------------|--------------|
| 1004 | Product_1004 | Electronics | 6 | 2959.80 | 0.00 |
| 1022 | Product_1022 | Footwear | 7 | 1740.69 | 0.00 |
| 1009 | Product_1009 | Accessories | 4 | 1470.00 | 0.00 |
| 1021 | Product_1021 | Electronics | 6 | 1325.40 | 0.00 |
| 1005 | Product_1005 | Home | 4 | 1117.24 | 0.00 |
| 1010 | Product_1010 | Electronics | 2 | 966.54 | 0.00 |
| 1007 | Product_1007 | Accessories | 2 | 862.24 | 0.00 |
| 1025 | Product_1025 | Apparel | 5 | 630.95 | 0.00 |
| 1020 | Product_1020 | Home | 3 | 515.16 | 0.00 |
| 1014 | Product_1014 | Apparel | 5 | 496.95 | 0.00 |
| 1018 | Product_1018 | Electronics | 3 | 465.42 | 0.00 |
| 1029 | Product_1029 | Footwear | 3 | 286.62 | 0.00 |
| 1024 | Product_1024 | Apparel | 1 | 265.41 | 0.00 |
| 1015 | Product_1015 | Accessories | 1 | 154.40 | 0.00 |
| 1026 | Product_1026 | Footwear | 8 | 150.24 | 0.00 |
| 1006 | Product_1006 | Accessories | 4 | 112.60 | 0.00 |

> `return_rate` currently fixed at `0.00`.  
> Replace with real computation if return data becomes available.

---
