-- Test vectorized CASE WHEN in vec_project
CREATE TABLE t_case (id INT, score INT, name TEXT);
INSERT INTO t_case VALUES (1, 95, 'Alice');
INSERT INTO t_case VALUES (2, 75, 'Bob');
INSERT INTO t_case VALUES (3, 55, 'Carol');
INSERT INTO t_case VALUES (4, 35, 'Dave');

-- CASE WHEN with multiple branches + col passthrough
SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 70 THEN 'B' WHEN score >= 50 THEN 'C' ELSE 'F' END AS grade FROM t_case ORDER BY id;

-- CASE WHEN with no else (returns NULL)
SELECT id, CASE WHEN score >= 90 THEN 'excellent' END FROM t_case ORDER BY id;

-- CASE WHEN returning numeric
SELECT id, CASE WHEN score >= 70 THEN 1 ELSE 0 END AS passed FROM t_case ORDER BY id;
