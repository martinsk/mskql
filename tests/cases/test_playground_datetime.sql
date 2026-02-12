-- playground example: date/time arithmetic and functions
-- input:
SELECT '2024-01-15'::date + 30 AS plus_30_days, EXTRACT(MONTH FROM '2024-03-15'::date) AS month, DATE_TRUNC('month', '2024-03-15'::date) AS trunc_month;
-- expected output:
2024-02-14|3|2024-03-01 00:00:00
-- expected status: 0
