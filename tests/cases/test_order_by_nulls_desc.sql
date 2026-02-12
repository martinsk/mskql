-- ORDER BY DESC with NULL values - NULLs should sort first in DESC
-- setup:
CREATE TABLE t1 (id INT, val INT);
INSERT INTO t1 VALUES (1, 30), (2, NULL), (3, 10), (4, NULL), (5, 20);
-- input:
SELECT id, val FROM t1 ORDER BY val DESC;
-- expected output:
2|
4|
1|30
5|20
3|10
-- expected status: 0
