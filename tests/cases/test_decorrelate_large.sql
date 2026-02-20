-- Correlated subquery decorrelation: larger dataset (benchmark pattern)
CREATE TABLE csq_master (id INT, val INT);
CREATE TABLE csq_detail (id INT, ref_id INT, score INT);
INSERT INTO csq_master SELECT n, n * 3 FROM generate_series(0, 199) AS g(n);
INSERT INTO csq_detail SELECT n, n % 200, (n * 17) % 1000 FROM generate_series(0, 999) AS g(n);
SELECT csq_master.id, csq_master.val, (SELECT MAX(csq_detail.score) FROM csq_detail WHERE csq_detail.ref_id = csq_master.id) AS max_score FROM csq_master WHERE csq_master.val > 300;
DROP TABLE csq_detail;
DROP TABLE csq_master;
