-- adversarial: CASE WHEN with no matching branch and no ELSE — should return NULL
-- setup:
CREATE TABLE t_cwn (v INT);
INSERT INTO t_cwn VALUES (99);
-- input:
SELECT CASE WHEN v = 1 THEN 'one' WHEN v = 2 THEN 'two' END FROM t_cwn;
-- expected output:

