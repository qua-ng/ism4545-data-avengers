-- Task 2.3 count verification
SELECT 'dim_customer' AS table_name, COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM fact_sales;

-- Task 2.3 latest warehouse rows
SELECT f.sale_key, d.full_date, c.first_name, c.last_name,
       p.product_name, f.quantity, f.total_price
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_product p ON f.product_key = p.product_key
ORDER BY f.sale_key DESC
LIMIT 3;

-- Task 3.1 revenue by state on the Southeast shard
SELECT c.state, SUM(o.total_price) AS total_revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.state
ORDER BY total_revenue DESC;

-- Task 3.1 revenue by state on the Northeast shard
SELECT c.state, SUM(o.total_price) AS total_revenue
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.state
ORDER BY total_revenue DESC;

-- Task 4.1 EXPLAIN ANALYZE on sales-shard-se
EXPLAIN ANALYZE
SELECT p.category,
       COUNT(*) AS order_count,
       SUM(o.total_price) AS total_revenue
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;

-- Task 4.2 EXPLAIN ANALYZE on warehouse-db
EXPLAIN ANALYZE
SELECT p.category,
       COUNT(*) AS order_count,
       SUM(f.total_price) AS total_revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;
