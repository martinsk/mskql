-- test plan having
-- setup:
CREATE TABLE test_hav (category TEXT, amount INT);
INSERT INTO test_hav VALUES ('a', 10), ('a', 20), ('b', 5), ('b', 3), ('c', 100);
-- input:
SELECT category, COUNT(*) AS cnt FROM test_hav GROUP BY category HAVING COUNT(*) > 1
EXPLAIN SELECT category, COUNT(*) AS cnt FROM test_hav GROUP BY category HAVING COUNT(*) > 1

-- expected output:
a|2
b|2
Filter: (count > 1)
  HashAggregate
    Seq Scan on test_hav

-- expected status: 0
