-- subquery with compound WHERE: IN-subquery AND simple filter + ORDER BY + LIMIT
-- setup:
CREATE TABLE customers (id INT, name TEXT, tier TEXT);
INSERT INTO customers VALUES (1, 'alice', 'premium');
INSERT INTO customers VALUES (2, 'bob', 'basic');
INSERT INTO customers VALUES (3, 'carol', 'premium');
INSERT INTO customers VALUES (4, 'dave', 'basic');
INSERT INTO customers VALUES (5, 'eve', 'premium');
CREATE TABLE events (id INT, user_id INT, amount INT);
INSERT INTO events VALUES (1, 1, 50);
INSERT INTO events VALUES (2, 1, 200);
INSERT INTO events VALUES (3, 2, 300);
INSERT INTO events VALUES (4, 3, 150);
INSERT INTO events VALUES (5, 3, 80);
INSERT INTO events VALUES (6, 4, 500);
INSERT INTO events VALUES (7, 5, 250);
INSERT INTO events VALUES (8, 5, 90);
-- input:
SELECT * FROM events WHERE user_id IN (SELECT id FROM customers WHERE tier = 'premium') AND amount > 100 ORDER BY amount DESC LIMIT 3;
-- expected output:
7|5|250
2|1|200
4|3|150
