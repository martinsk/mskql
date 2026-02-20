-- Vectorized ABS
CREATE TABLE vec_abs (id INT, score INT, big BIGINT, val FLOAT);
INSERT INTO vec_abs VALUES (1, -50, -100, -3.14), (2, 30, 200, 2.71), (3, 0, 0, 0.0), (4, -1, -9999999999, -0.001);
SELECT ABS(score) FROM vec_abs;
SELECT ABS(big) FROM vec_abs;
SELECT ABS(val) FROM vec_abs;
SELECT id, ABS(score), ABS(val) FROM vec_abs;
DROP TABLE vec_abs;
