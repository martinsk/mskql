-- Test: join without WHERE — no pushdown, should still work
-- Setup
CREATE TABLE pd_nw_a (id INT PRIMARY KEY, val TEXT);
INSERT INTO pd_nw_a VALUES (1, 'x');
INSERT INTO pd_nw_a VALUES (2, 'y');

CREATE TABLE pd_nw_b (id INT PRIMARY KEY, a_id INT, data TEXT);
INSERT INTO pd_nw_b VALUES (1, 1, 'foo');
INSERT INTO pd_nw_b VALUES (2, 2, 'bar');

-- Input
SELECT a.val, b.data
FROM pd_nw_a a
JOIN pd_nw_b b ON a.id = b.a_id
ORDER BY a.val;

-- Expected
-- x|foo
-- y|bar
