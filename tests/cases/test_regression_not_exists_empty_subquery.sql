-- BUG: NOT EXISTS with subquery returning no rows should return all rows
-- setup:
CREATE TABLE t (id INT, val TEXT);
INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c');
-- input:
SELECT * FROM t WHERE NOT EXISTS (SELECT 1 WHERE 1=0) ORDER BY id;
-- expected output:
1|a
2|b
3|c
-- expected status: 0
