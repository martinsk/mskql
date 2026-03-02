-- test plan expr proj distinct
-- setup:
CREATE TABLE test_epd (id INT, name TEXT);
INSERT INTO test_epd VALUES (1, 'alice'), (2, 'bob'), (3, 'alice'), (4, 'bob'), (5, 'charlie');
-- input:
SELECT DISTINCT UPPER(name) FROM test_epd
EXPLAIN SELECT DISTINCT UPPER(name) FROM test_epd

-- expected output:
ALICE
BOB
CHARLIE
HashAggregate (DISTINCT)
  Vec Project
    Seq Scan on test_epd

-- expected status: 0
