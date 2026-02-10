-- DISTINCT should treat multiple NULL rows as duplicates (SQL standard)
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 (id, val) VALUES (1, NULL), (2, NULL), (3, 10), (4, 10);
-- input:
SELECT DISTINCT val FROM t1 ORDER BY val;
-- expected output:
10
-- expected status: 0
