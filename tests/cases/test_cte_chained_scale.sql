-- Chained CTEs where the second CTE reads from the first.
-- At moderate scale this exercises the CTE-to-CTE reference path.
-- setup:
CREATE TABLE cte_events (id INT, user_id INT, event_type INT, amount INT);
INSERT INTO cte_events SELECT n, n % 50, n % 3, (n * 11) % 100 FROM generate_series(0, 499) AS g(n);
-- input:
WITH totals AS (SELECT user_id, SUM(amount) AS total FROM cte_events WHERE event_type = 0 GROUP BY user_id), big AS (SELECT * FROM totals WHERE total > 100) SELECT COUNT(*) FROM big;
-- expected output:
49
-- expected status: 0
