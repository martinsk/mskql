-- CHECK constraint rejects invalid insert
-- setup:
CREATE TABLE t1 (id INT, price INT CHECK(price > 0));
-- input:
INSERT INTO t1 (id, price) VALUES (1, -5);
-- expected output:
ERROR:  CHECK constraint violated for column 'price'
-- expected status: 1
