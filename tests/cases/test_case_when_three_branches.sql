-- CASE WHEN with three branches and ELSE
-- setup:
CREATE TABLE t1 (id INT, score INT);
INSERT INTO t1 (id, score) VALUES (1, 90), (2, 70), (3, 50), (4, 30);
-- input:
SELECT id, CASE WHEN score >= 80 THEN 'A' WHEN score >= 60 THEN 'B' WHEN score >= 40 THEN 'C' ELSE 'F' END FROM t1 ORDER BY id;
-- expected output:
1|A
2|B
3|C
4|F
-- expected status: 0
