-- CHECK constraint allows valid insert
-- setup:
CREATE TABLE t1 (id INT, price INT CHECK(price > 0));
-- input:
INSERT INTO t1 (id, price) VALUES (1, 10);
SELECT id, price FROM t1;
-- expected output:
INSERT 0 1
1|10
-- expected status: 0
