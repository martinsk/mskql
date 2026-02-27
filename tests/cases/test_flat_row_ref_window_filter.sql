-- Regression: eval_expr_flat via window_filter_passes (flat_row_ref path)
-- Exercises window function with FILTER clause evaluated from flat window data.
-- setup:
CREATE TABLE frr_win (category TEXT, amount INT);
INSERT INTO frr_win (category, amount) VALUES ('A', 10), ('A', 20), ('A', 5), ('B', 30), ('B', 15), ('B', 3);
-- input:
SELECT category, amount, SUM(amount) FILTER (WHERE amount > 10) OVER (PARTITION BY category) AS big_sum FROM frr_win ORDER BY category, amount;
-- expected output:
A|5|20
A|10|20
A|20|20
B|3|45
B|15|45
B|30|45
-- expected status: 0
