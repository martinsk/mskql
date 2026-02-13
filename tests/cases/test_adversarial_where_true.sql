-- adversarial: WHERE clause that is always true
-- BUG: parser rejects WHERE with literal-only comparisons (WHERE 1 = 1)
-- because it expects a column name on the LHS. PostgreSQL supports this.
-- setup:
CREATE TABLE t_wt (id INT);
INSERT INTO t_wt VALUES (1);
INSERT INTO t_wt VALUES (2);
-- input:
SELECT * FROM t_wt WHERE 1 = 1;
-- expected status: error
