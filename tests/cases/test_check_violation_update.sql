-- CHECK constraint rejects invalid update
-- setup:
CREATE TABLE t1 (id INT, price INT CHECK(price > 0));
INSERT INTO t1 (id, price) VALUES (1, 10);
-- input:
UPDATE t1 SET price = -1 WHERE id = 1;
-- expected output:
ERROR:  CHECK constraint violated for column 'price'
-- expected status: 1
