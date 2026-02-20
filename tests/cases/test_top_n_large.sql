-- Top-N Sort: large dataset to verify correctness at scale
CREATE TABLE top_n_lg (id INT, val INT);
INSERT INTO top_n_lg SELECT n, (n * 31337) % 10000 FROM generate_series(1, 5000) AS g(n);
SELECT id, val FROM top_n_lg ORDER BY val ASC LIMIT 5;
SELECT id, val FROM top_n_lg ORDER BY val DESC LIMIT 5;
SELECT id, val FROM top_n_lg ORDER BY val ASC LIMIT 5 OFFSET 10;
SELECT COUNT(*) FROM (SELECT id FROM top_n_lg ORDER BY val LIMIT 100) sub;
DROP TABLE top_n_lg;
