--Monthly Sales Summary
SELECT d.month_name,
       d.year,
       COUNT(*) AS total_orders,
       SUM(f.total_price) AS total_revenue
FROM fact_sales f
JOIN dim_date d
  ON f.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

--Top Customers by Spending
SELECT c.first_name,
       c.last_name,
       COUNT(*) AS number_of_orders,
       SUM(f.total_price) AS total_amount_spent
FROM fact_sales f
JOIN dim_customer c
  ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_amount_spent DESC;

--Revenue by Product Category and Month
SELECT p.category,
       d.month_name,
       d.year,
       SUM(f.total_price) AS total_revenue
FROM fact_sales f
JOIN dim_product p
  ON f.product_key = p.product_key
JOIN dim_date d
  ON f.date_key = d.date_key
GROUP BY p.category, d.year, d.month, d.month_name
ORDER BY d.year, d.month, p.category;