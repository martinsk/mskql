-- NOT LIKE should exclude NULL rows
-- setup:
CREATE TABLE t (id INT, name TEXT);
INSERT INTO t VALUES (1, 'alice'), (2, 'bob'), (3, NULL);
-- input:
SELECT * FROM t WHERE name NOT LIKE 'a%' ORDER BY id;
-- expected output:
2|bob
-- expected status: 0
