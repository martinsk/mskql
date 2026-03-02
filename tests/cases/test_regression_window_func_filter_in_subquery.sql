-- Bug: window function result cannot be filtered via outer WHERE when subquery aliases it
-- SELECT rn FROM (...ROW_NUMBER() OVER (...) AS rn...) sub WHERE rn = 2
-- returns error: column "rn" does not exist
-- In PostgreSQL this is a standard pattern and must work
-- setup:
CREATE TABLE t_wfilt (n INT);
INSERT INTO t_wfilt VALUES (1),(2),(3),(4),(5);
-- input:
SELECT rn FROM (SELECT n, ROW_NUMBER() OVER (ORDER BY n) AS rn FROM t_wfilt) sub WHERE rn = 3;
-- expected output:
3
-- expected status: 0
