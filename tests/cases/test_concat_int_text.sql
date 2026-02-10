-- string concat with integer column should coerce int to text
-- setup:
CREATE TABLE t1 (id INT, name TEXT, age INT);
INSERT INTO t1 (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25);
-- input:
SELECT id, name || ' is ' || age FROM t1 ORDER BY id;
-- expected output:
1|Alice is 30
2|Bob is 25
-- expected status: 0
