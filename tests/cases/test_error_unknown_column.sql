-- GROUP BY on nonexistent column should report column name
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 (id, name) VALUES (1, 'alice');
-- input:
SELECT COUNT(*) FROM t1 GROUP BY bogus;
-- expected output:
ERROR:  GROUP BY column 'bogus' not found
-- expected status: 1
