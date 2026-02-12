-- multi-column IN with mismatched tuple width
-- setup:
CREATE TABLE t1 (a INT, b INT, c INT);
INSERT INTO t1 VALUES (1, 2, 3), (4, 5, 6);
-- input:
SELECT * FROM t1 WHERE (a, b) IN ((1, 2), (4, 5));
-- expected output:
1|2|3
4|5|6
-- expected status: 0
