-- Test: predicate pushdown for LEFT JOIN — only inner (right) side filtered
-- Setup
CREATE TABLE pd_left_users (id INT PRIMARY KEY, name TEXT);
INSERT INTO pd_left_users VALUES (1, 'Alice');
INSERT INTO pd_left_users VALUES (2, 'Bob');
INSERT INTO pd_left_users VALUES (3, 'Carol');

CREATE TABLE pd_left_orders (id INT PRIMARY KEY, user_id INT, status TEXT);
INSERT INTO pd_left_orders VALUES (1, 1, 'shipped');
INSERT INTO pd_left_orders VALUES (2, 1, 'pending');
INSERT INTO pd_left_orders VALUES (3, 2, 'shipped');
INSERT INTO pd_left_orders VALUES (4, 3, 'cancelled');

-- Input: filter on inner (right) side only — should be pushed down
SELECT u.name, o.status
FROM pd_left_users u
LEFT JOIN pd_left_orders o ON u.id = o.user_id
WHERE o.status = 'shipped'
ORDER BY u.name;

-- Expected
-- Alice|shipped
-- Bob|shipped
