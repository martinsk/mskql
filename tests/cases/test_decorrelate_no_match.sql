-- Correlated subquery decorrelation: NULL when no matching rows (LEFT JOIN behavior)
CREATE TABLE dec_m4 (id INT, val INT);
CREATE TABLE dec_d4 (id INT, ref_id INT, score INT);
INSERT INTO dec_m4 VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO dec_d4 VALUES (1, 1, 100), (2, 1, 200);
SELECT dec_m4.id, dec_m4.val, (SELECT MAX(dec_d4.score) FROM dec_d4 WHERE dec_d4.ref_id = dec_m4.id) AS max_score FROM dec_m4;
-- expect: id=2 and id=3 should have NULL max_score
DROP TABLE dec_d4;
DROP TABLE dec_m4;
