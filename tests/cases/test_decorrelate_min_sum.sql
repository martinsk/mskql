-- Correlated subquery decorrelation: MIN and SUM aggregates
CREATE TABLE dec_m3 (id INT, name TEXT);
CREATE TABLE dec_d3 (id INT, ref_id INT, amount INT);
INSERT INTO dec_m3 VALUES (1, 'a'), (2, 'b'), (3, 'c');
INSERT INTO dec_d3 VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, 50);
SELECT dec_m3.id, (SELECT MIN(dec_d3.amount) FROM dec_d3 WHERE dec_d3.ref_id = dec_m3.id) AS min_amt FROM dec_m3;
SELECT dec_m3.id, (SELECT SUM(dec_d3.amount) FROM dec_d3 WHERE dec_d3.ref_id = dec_m3.id) AS total FROM dec_m3;
DROP TABLE dec_d3;
DROP TABLE dec_m3;
