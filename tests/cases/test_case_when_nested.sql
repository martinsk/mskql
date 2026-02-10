-- CASE WHEN with multiple WHEN branches
-- setup:
CREATE TABLE t1 (id INT, score INT);
INSERT INTO t1 (id, score) VALUES (1, 95), (2, 75), (3, 55), (4, 35);
-- input:
SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' WHEN score >= 50 THEN 'C' ELSE 'F' END FROM t1 ORDER BY id;
-- expected output:
1|A
2|B
3|C
4|F
-- expected status: 0
