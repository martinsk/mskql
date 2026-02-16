-- plan: WHERE with IN literal list on text column
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave');
-- input:
SELECT id FROM t1 WHERE name IN ('bob', 'dave') ORDER BY id;
-- expected output:
2
4
-- expected status: 0
