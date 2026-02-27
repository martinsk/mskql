-- Regression: eval_expr_col via expr_project_next (col_row_ref path)
-- Exercises EXPR_COLUMN_REF resolution from col_blocks in expression projection.
-- setup:
CREATE TABLE crr_proj (a INT, b INT, label TEXT);
INSERT INTO crr_proj (a, b, label) VALUES (10, 3, 'x'), (20, 7, 'y'), (5, 2, 'z');
-- input:
SELECT a * b + 1, UPPER(label) FROM crr_proj ORDER BY a;
-- expected output:
11|Z
31|X
141|Y
-- expected status: 0
