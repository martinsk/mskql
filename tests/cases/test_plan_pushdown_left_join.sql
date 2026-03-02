-- plan: pushdown left join filter
-- setup:
CREATE TABLE pd_left_users (id INT PRIMARY KEY, name TEXT);
INSERT INTO pd_left_users VALUES (1, 'Alice');
INSERT INTO pd_left_users VALUES (2, 'Bob');
INSERT INTO pd_left_users VALUES (3, 'Carol');
CREATE TABLE pd_left_orders (id INT PRIMARY KEY, user_id INT, status TEXT);
INSERT INTO pd_left_orders VALUES (1, 1, 'shipped');
INSERT INTO pd_left_orders VALUES (2, 1, 'pending');
INSERT INTO pd_left_orders VALUES (3, 2, 'shipped');
INSERT INTO pd_left_orders VALUES (4, 3, 'cancelled');
-- input:
SELECT u.name, o.status FROM pd_left_users u LEFT JOIN pd_left_orders o ON u.id = o.user_id WHERE o.status = 'shipped' ORDER BY u.name
EXPLAIN SELECT u.name, o.status FROM pd_left_users u LEFT JOIN pd_left_orders o ON u.id = o.user_id WHERE o.status = 'shipped' ORDER BY u.name
-- expected output:
Alice|shipped
Bob|shipped
Project
  Sort
    Filter: (o.status = 'shipped')
      Hash Join
        Seq Scan on pd_left_users
        Seq Scan on pd_left_orders
-- expected status: 0
