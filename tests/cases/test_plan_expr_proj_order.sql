-- test plan expr proj order
-- setup:
CREATE TABLE test_epo (id INT, name TEXT, score INT);
INSERT INTO test_epo VALUES (1, 'alice', 30), (2, 'bob', 10), (3, 'charlie', 20);
-- input:
SELECT UPPER(name), score FROM test_epo ORDER BY score
EXPLAIN SELECT UPPER(name), score FROM test_epo ORDER BY score

-- expected output:
BOB|10
CHARLIE|20
ALICE|30
Vec Project
  Sort (score)
    Seq Scan on test_epo

-- expected status: 0
