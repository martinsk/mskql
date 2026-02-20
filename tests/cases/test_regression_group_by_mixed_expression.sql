-- BUG: GROUP BY with multi-column list including expression errors with 'expected FROM'
-- setup:
CREATE TABLE t (id INT, category TEXT, amount INT);
INSERT INTO t VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'B', 40);
-- input:
SELECT category, amount / 10 AS bucket, COUNT(*) FROM t GROUP BY category, amount / 10 ORDER BY category, bucket;
-- expected output:
A|1|1
A|3|1
B|2|1
B|4|1
-- expected status: 0
