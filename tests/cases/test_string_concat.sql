-- string concatenation with || operator
-- setup:
CREATE TABLE "t1" (id INT, first_name TEXT, last_name TEXT);
INSERT INTO "t1" (id, first_name, last_name) VALUES (1, 'Alice', 'Smith'), (2, 'Bob', 'Jones');
-- input:
SELECT id, first_name || ' ' || last_name FROM "t1" ORDER BY id;
-- expected output:
1|Alice Smith
2|Bob Jones
-- expected status: 0
