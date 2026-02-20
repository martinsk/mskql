-- Vectorized ROUND
CREATE TABLE vec_rnd (id INT, score INT, val FLOAT);
INSERT INTO vec_rnd VALUES (1, 42, 3.14159), (2, -7, 2.71828), (3, 100, 0.005), (4, 0, -1.999);
SELECT ROUND(val::numeric, 2) FROM vec_rnd;
SELECT ROUND(score::numeric, 0) FROM vec_rnd;
SELECT id, ROUND(val::numeric, 2) FROM vec_rnd;
DROP TABLE vec_rnd;
