-- test plan window text lag
-- setup:
CREATE TABLE test_wt (id INT, name TEXT);
INSERT INTO test_wt VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie');
-- input:
SELECT name, LAG(name) OVER (ORDER BY id) AS prev_name FROM test_wt
EXPLAIN SELECT name, LAG(name) OVER (ORDER BY id) AS prev_name FROM test_wt

-- expected output:
alice|
bob|alice
charlie|bob
WindowAgg
  Seq Scan on test_wt

-- expected status: 0
