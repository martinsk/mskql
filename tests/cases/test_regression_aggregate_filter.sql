-- regression: aggregate FILTER clause
-- setup:
CREATE TABLE t (grp TEXT, val INT);
INSERT INTO t VALUES ('a',10),('b',20),('a',30),('b',40);
-- input:
SELECT COUNT(*) FILTER (WHERE grp = 'a'), COUNT(*) FILTER (WHERE grp = 'b') FROM t;
-- expected output:
2|2
