-- plan: simple aggregate functions
-- setup:
CREATE TABLE test_sa (id INT, val INT, score FLOAT);
INSERT INTO test_sa VALUES (1, 10, 1.5), (2, 20, 2.5), (3, 30, 3.5), (4, NULL, NULL);
-- input:
SELECT COUNT(*), MIN(val), MAX(val), SUM(val) FROM test_sa
EXPLAIN SELECT COUNT(*), MIN(val), MAX(val), SUM(val) FROM test_sa
-- expected output:
4|10|30|60
Aggregate
  Seq Scan on test_sa
-- expected status: 0
