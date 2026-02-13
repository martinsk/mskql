-- adversarial: DELETE all rows then SELECT — should not crash
-- setup:
CREATE TABLE t_das (id INT, name TEXT);
INSERT INTO t_das VALUES (1, 'a');
INSERT INTO t_das VALUES (2, 'b');
DELETE FROM t_das;
-- input:
SELECT COUNT(*) FROM t_das;
-- expected output:
0
