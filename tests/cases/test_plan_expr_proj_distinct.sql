CREATE TABLE test_epd (id INT, name TEXT);
INSERT INTO test_epd VALUES (1, 'alice'), (2, 'bob'), (3, 'alice'), (4, 'bob'), (5, 'charlie');
SELECT DISTINCT UPPER(name) FROM test_epd;
-- EXPECT: 3 rows
-- ALICE
-- BOB
-- CHARLIE
DROP TABLE test_epd;
