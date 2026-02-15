-- regression: GROUP BY output columns in correct order
-- setup:
CREATE TABLE t (name TEXT, score INT);
INSERT INTO t VALUES ('alice',10),('bob',20),('alice',30);
-- input:
SELECT name, SUM(score) as total FROM t GROUP BY name ORDER BY name;
-- expected output:
alice|40
bob|20
