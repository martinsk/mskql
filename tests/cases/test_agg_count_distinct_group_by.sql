-- COUNT(DISTINCT col) with GROUP BY
-- setup:
CREATE TABLE t1 (dept TEXT, name TEXT);
INSERT INTO t1 VALUES ('eng', 'alice');
INSERT INTO t1 VALUES ('eng', 'bob');
INSERT INTO t1 VALUES ('eng', 'alice');
INSERT INTO t1 VALUES ('sales', 'carol');
INSERT INTO t1 VALUES ('sales', 'carol');
-- input:
SELECT dept, COUNT(DISTINCT name) FROM t1 GROUP BY dept ORDER BY dept;
-- expected output:
eng|3
sales|2
-- expected status: 0
