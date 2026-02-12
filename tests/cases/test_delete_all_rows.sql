-- DELETE without WHERE deletes all rows
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- input:
DELETE FROM t1;
SELECT COUNT(*) FROM t1;
-- expected output:
DELETE 3
0
-- expected status: 0
