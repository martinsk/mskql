-- plan: WHERE with IN literal list
-- setup:
CREATE TABLE t1 (id INT, name TEXT);
INSERT INTO t1 VALUES (1, 'alice'), (2, 'bob'), (3, 'carol'), (4, 'dave'), (5, 'eve');
-- input:
SELECT name FROM t1 WHERE id IN (2, 4, 5) ORDER BY id;
-- expected output:
bob
dave
eve
-- expected status: 0
