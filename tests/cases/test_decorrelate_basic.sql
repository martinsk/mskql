-- Correlated subquery decorrelation: basic MAX pattern
CREATE TABLE dec_master (id INT, val INT);
CREATE TABLE dec_detail (id INT, ref_id INT, score INT);
INSERT INTO dec_master VALUES (1, 10), (2, 20), (3, 30);
INSERT INTO dec_detail VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150), (4, 2, 50), (5, 3, 300);
SELECT dec_master.id, dec_master.val, (SELECT MAX(dec_detail.score) FROM dec_detail WHERE dec_detail.ref_id = dec_master.id) AS max_score FROM dec_master;
DROP TABLE dec_detail;
DROP TABLE dec_master;
