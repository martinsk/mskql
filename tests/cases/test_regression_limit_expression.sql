-- BUG: LIMIT with expression (2 + 1) should evaluate to 3, not treat as literal 2
-- setup:
CREATE TABLE t (id INT);
INSERT INTO t VALUES (1),(2),(3),(4),(5);
-- input:
SELECT * FROM t ORDER BY id LIMIT 2 + 1;
-- expected output:
1
2
3
-- expected status: 0
