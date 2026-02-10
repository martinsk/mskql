-- CASE WHEN with greater-than comparison
-- setup:
CREATE TABLE t1 (id INT, score INT);
INSERT INTO t1 (id, score) VALUES (1, 90), (2, 50), (3, 75);
-- input:
SELECT id, CASE WHEN score >= 80 THEN 'pass' WHEN score >= 60 THEN 'ok' ELSE 'fail' END FROM t1 ORDER BY id;
-- expected output:
1|pass
2|fail
3|ok
-- expected status: 0
