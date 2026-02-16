-- Test COUNT(CASE WHEN ...) through plan executor
-- setup:
CREATE TABLE t_cc (id INT, status TEXT, amount INT);
INSERT INTO t_cc VALUES (1, 'paid', 100);
INSERT INTO t_cc VALUES (2, 'pending', 200);
INSERT INTO t_cc VALUES (3, 'paid', 150);
INSERT INTO t_cc VALUES (4, 'refund', 50);
INSERT INTO t_cc VALUES (5, 'paid', 300);
-- input:
SELECT COUNT(CASE WHEN status = 'paid' THEN 1 END) FROM t_cc;
-- expected output:
3
