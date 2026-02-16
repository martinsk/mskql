CREATE TABLE test_sa (id INT, val INT, score FLOAT);
INSERT INTO test_sa VALUES (1, 10, 1.5), (2, 20, 2.5), (3, 30, 3.5), (4, NULL, NULL);
SELECT COUNT(*) FROM test_sa;
-- EXPECT: 4
SELECT COUNT(val) FROM test_sa;
-- EXPECT: 3
SELECT SUM(val) FROM test_sa;
-- EXPECT: 60
SELECT AVG(score) FROM test_sa;
-- EXPECT: 2.5
SELECT MIN(val), MAX(val) FROM test_sa;
-- EXPECT: 10|30
DROP TABLE test_sa;
