-- adversarial: WHERE clause that is always false
-- BUG: parser rejects WHERE with literal-only comparisons (WHERE 1 = 0)
-- because it expects a column name on the LHS. PostgreSQL supports this.
-- setup:
CREATE TABLE t_wf (id INT);
INSERT INTO t_wf VALUES (1);
INSERT INTO t_wf VALUES (2);
-- input:
SELECT * FROM t_wf WHERE 1 = 0;
-- expected status: error
