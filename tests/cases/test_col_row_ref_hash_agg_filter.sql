-- Regression: eval_condition_col via hash_agg_next FILTER (col_row_ref path)
-- Exercises per-aggregate FILTER condition evaluated from col_blocks.
-- setup:
CREATE TABLE crr_hagg (category TEXT, amount INT);
INSERT INTO crr_hagg (category, amount) VALUES ('A', 10), ('A', 20), ('A', 5), ('B', 30), ('B', 15), ('B', 3);
-- input:
SELECT category, SUM(amount) FILTER (WHERE amount > 10) AS big_sum, COUNT(*) FILTER (WHERE amount <= 10) AS small_cnt FROM crr_hagg GROUP BY category ORDER BY category;
-- expected output:
A|20|2
B|45|1
-- expected status: 0
