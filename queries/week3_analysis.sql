-- Daily active users by country 
SELECT
  partition_date,
  location AS country,
  COUNT(DISTINCT user_id) AS daily_active_users
FROM `dataengineering-internship.bigquery_testing.clickstream`
GROUP BY partition_date, location
ORDER BY partition_date, country;
-- Result: Highest active users on 2025-09-03 in US


-- Revenue per currency vs USD (last 30 days)
SELECT
  currency,
  SUM(amount_usd) AS total_revenue_usd
FROM `dataengineering-internship.bigquery_testing.transactions`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY currency
ORDER BY total_revenue_usd DESC;
-- Result: Highest total revenue in EUR (3,190,976)

-- Funnel from visits -> carts -> transactions
-- distinct session ids, 'cart' in page_url, distinct transactions
-- Joined on user_id and partition_date for alignment
SELECT
  c.partition_date,
  COUNT(DISTINCT c.session_id) AS visits,
  COUNT(DISTINCT CASE WHEN c.page_url LIKE '%cart%' THEN c.user_id END) AS carts,
  COUNT(DISTINCT t.txn_id) AS transactions
FROM `dataengineering-internship.bigquery_testing.clickstream` c
LEFT JOIN `dataengineering-internship.bigquery_testing.transactions` t
  ON c.user_id = t.user_id AND c.partition_date = t.partition_date
GROUP BY c.partition_date
ORDER BY c.partition_date;

-- Compare costs: Full table scan (without partition filter)
-- This scans the entire clickstream table
SELECT COUNT(*) AS total_rows
FROM `dataengineering-internship.bigquery_testing.clickstream`;

-- Compare costs: With partition filter (last 30 days)
-- This scans only the partitions for the last 30 days, potentially reducing cost
SELECT COUNT(*) AS total_rows_last_30_days
FROM `dataengineering-internship.bigquery_testing.clickstream`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY);