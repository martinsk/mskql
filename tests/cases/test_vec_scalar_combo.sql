-- Vectorized scalar function combo (the benchmark query pattern)
CREATE TABLE vec_combo (id INT, name TEXT, score INT);
INSERT INTO vec_combo VALUES (1, 'alice', 50), (2, 'Bob', -30), (3, 'CHARLIE', 0), (4, 'dave', -99);
SELECT UPPER(name), LENGTH(name), ABS(score), ROUND(score::numeric, 2) FROM vec_combo;
DROP TABLE vec_combo;
