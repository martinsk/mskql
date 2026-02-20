-- BUG: ORDER BY on enum column crashes the server
-- setup:
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE t (id INT, feeling mood);
INSERT INTO t VALUES (1, 'happy'), (2, 'sad'), (3, 'ok');
-- input:
SELECT * FROM t ORDER BY feeling;
-- expected output:
2|sad
3|ok
1|happy
-- expected status: 0
