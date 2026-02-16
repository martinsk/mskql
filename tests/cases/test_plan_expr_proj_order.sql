CREATE TABLE test_epo (id INT, name TEXT, score INT);
INSERT INTO test_epo VALUES (1, 'alice', 30), (2, 'bob', 10), (3, 'charlie', 20);
SELECT UPPER(name), score FROM test_epo ORDER BY score;
-- EXPECT:
-- BOB|10
-- CHARLIE|20
-- ALICE|30
DROP TABLE test_epo;
