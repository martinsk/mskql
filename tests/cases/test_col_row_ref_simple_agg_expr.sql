-- Regression: eval_expr_col via simple_agg_next (col_row_ref path)
-- Exercises expression-based aggregate without GROUP BY evaluated from col_blocks.
-- setup:
CREATE TABLE crr_sagg (price INT, qty INT, active BOOLEAN);
INSERT INTO crr_sagg (price, qty, active) VALUES (10, 3, true), (20, 2, false), (5, 10, true), (8, 4, true);
-- input:
SELECT SUM(price * qty), AVG(price * qty) FILTER (WHERE active = true) FROM crr_sagg;
-- expected output:
152|37.3333
-- expected status: 0
