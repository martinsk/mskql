-- BUG: NATURAL JOIN with multiple common columns only joins on first common column, ignores others
-- setup:
CREATE TABLE t1 (id INT, name TEXT, val INT);
CREATE TABLE t2 (id INT, name TEXT, score INT);
INSERT INTO t1 VALUES (1, 'Alice', 10), (2, 'Bob', 20);
INSERT INTO t2 VALUES (1, 'Alice', 100), (2, 'Charlie', 200);
-- input:
SELECT * FROM t1 NATURAL JOIN t2 ORDER BY id;
-- expected output:
1|Alice|10|100
-- expected status: 0
