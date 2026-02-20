-- Correlated subquery decorrelation: with WHERE on outer table
CREATE TABLE dec_m2 (id INT, val INT);
CREATE TABLE dec_d2 (id INT, ref_id INT, score INT);
INSERT INTO dec_m2 VALUES (1, 10), (2, 20), (3, 30), (4, 40);
INSERT INTO dec_d2 VALUES (1, 1, 100), (2, 2, 200), (3, 3, 300), (4, 4, 400);
SELECT dec_m2.id, dec_m2.val, (SELECT MAX(dec_d2.score) FROM dec_d2 WHERE dec_d2.ref_id = dec_m2.id) AS max_score FROM dec_m2 WHERE dec_m2.val > 20;
DROP TABLE dec_d2;
DROP TABLE dec_m2;
