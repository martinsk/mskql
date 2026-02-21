-- BUG: Multiple COUNT(DISTINCT col) aggregates in same query crashes the server
-- setup:
CREATE TABLE t (grp TEXT, a INT, b INT);
INSERT INTO t VALUES ('X', 1, 10), ('X', 1, 20), ('X', 2, 10), ('Y', 3, 30);
-- input:
SELECT grp, COUNT(DISTINCT a), COUNT(DISTINCT b) FROM t GROUP BY grp ORDER BY grp;
-- expected output:
X|2|2
Y|1|1
-- expected status: 0
