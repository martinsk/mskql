-- multi-column ORDER BY with mixed ASC/DESC
-- setup:
CREATE TABLE t1 (id INT, dept TEXT, salary INT);
INSERT INTO t1 (id, dept, salary) VALUES (1, 'eng', 100), (2, 'eng', 200), (3, 'sales', 150), (4, 'sales', 100);
-- input:
SELECT id, dept, salary FROM t1 ORDER BY dept ASC, salary DESC;
-- expected output:
2|eng|200
1|eng|100
3|sales|150
4|sales|100
-- expected status: 0
