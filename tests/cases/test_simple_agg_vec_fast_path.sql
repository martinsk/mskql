-- Test vectorized simple_agg accumulation (int_fast_path)
-- Covers SUM, COUNT, MIN, MAX, AVG on INT, BIGINT, FLOAT columns
-- setup
CREATE TABLE test_sagg_vec (a INT, b BIGINT, c FLOAT);
INSERT INTO test_sagg_vec VALUES (10, 100, 1.5);
INSERT INTO test_sagg_vec VALUES (20, 200, 2.5);
INSERT INTO test_sagg_vec VALUES (30, 300, 3.5);
INSERT INTO test_sagg_vec VALUES (NULL, NULL, NULL);
INSERT INTO test_sagg_vec VALUES (40, 400, 4.5);
-- input
SELECT COUNT(*), SUM(a), AVG(a), MIN(a), MAX(a) FROM test_sagg_vec;
-- expected
5|100|25|10|40
-- input
SELECT COUNT(*), SUM(b), MIN(b), MAX(b) FROM test_sagg_vec;
-- expected
5|1000|100|400
-- input
SELECT COUNT(*), SUM(c), MIN(c), MAX(c) FROM test_sagg_vec;
-- expected
5|12|1.5|4.5
-- input
SELECT COUNT(a), SUM(a) FROM test_sagg_vec;
-- expected
4|100
-- input
SELECT COUNT(*) FROM test_sagg_vec;
-- expected
5
