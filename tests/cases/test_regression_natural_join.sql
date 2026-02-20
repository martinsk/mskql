-- BUG: NATURAL JOIN returns duplicate rows and wrong column layout
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE t2 (id INT, city TEXT);
INSERT INTO t1 VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO t2 VALUES (1, 'NYC'), (3, 'LA');
-- input:
SELECT * FROM t1 NATURAL JOIN t2;
-- expected output:
1|Alice|NYC
-- expected status: 0
