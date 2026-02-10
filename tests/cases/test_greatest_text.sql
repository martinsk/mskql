-- GREATEST with text arguments should compare lexicographically
-- setup:
CREATE TABLE t1 (id INT, a TEXT, b TEXT);
INSERT INTO t1 (id, a, b) VALUES (1, 'apple', 'banana'), (2, 'zebra', 'ant');
-- input:
SELECT id, GREATEST(a, b) FROM t1 ORDER BY id;
-- expected output:
1|banana
2|zebra
-- expected status: 0
