-- Show table schema
\d+ retail;

-- Q1: Show first 10 rows
SELECT * FROM retail
LIMIT 10;

-- Q2: Check # of records
SELECT COUNT(*) AS total_records
FROM retail;

-- Q3: Number of clients (unique customer_id)
SELECT COUNT(DISTINCT customer_id) AS total_clients
FROM retail;

-- Q4: Invoice date range (max and min)
SELECT MAX(invoice_date) AS max_date,
       MIN(invoice_date) AS min_date
FROM retail;

-- Q5: Number of SKU/merchants (unique stock_code)
SELECT COUNT(DISTINCT stock_code) AS total_skus
FROM retail;

-- Q6: Average invoice amount excluding invoices with negative amount
SELECT AVG(invoice_total) AS avg_invoice_amount
FROM (
    SELECT invoice_no,
           SUM(quantity * unit_price) AS invoice_total
    FROM retail
    GROUP BY invoice_no
    HAVING SUM(quantity * unit_price) > 0
) AS invoice_totals;

-- Q7: Total revenue (sum of unit_price * quantity)
SELECT SUM(quantity * unit_price) AS total_revenue
FROM retail;

-- Q8: Total revenue by YYYYMM
SELECT 
    (EXTRACT(YEAR FROM invoice_date)::int * 100 + EXTRACT(MONTH FROM invoice_date)::int) AS yyyymm,
    SUM(quantity * unit_price) AS total_revenue
FROM retail
GROUP BY yyyymm
ORDER BY yyyymm;
