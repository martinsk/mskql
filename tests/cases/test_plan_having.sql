CREATE TABLE test_hav (category TEXT, amount INT);
INSERT INTO test_hav VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 3), ('c', 100);
SELECT category, COUNT(*) AS cnt FROM test_hav GROUP BY category HAVING COUNT(*) > 1;
-- EXPECT: 2 rows
-- a|2
-- b|2
DROP TABLE test_hav;
