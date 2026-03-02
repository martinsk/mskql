-- cross-table predicate stays as post-join filter
-- setup:
CREATE TABLE pd_cross_a (id INT PRIMARY KEY, val INT);
INSERT INTO pd_cross_a VALUES (1, 10);
INSERT INTO pd_cross_a VALUES (2, 20);
INSERT INTO pd_cross_a VALUES (3, 30);

CREATE TABLE pd_cross_b (id INT PRIMARY KEY, a_id INT, val INT);
INSERT INTO pd_cross_b VALUES (1, 1, 15);
INSERT INTO pd_cross_b VALUES (2, 2, 25);
INSERT INTO pd_cross_b VALUES (3, 3, 5);
-- input:
SELECT a.id, a.val, b.val
FROM pd_cross_a a
JOIN pd_cross_b b ON a.id = b.a_id
WHERE a.val > 10
ORDER BY a.id;
EXPLAIN SELECT a.id, a.val, b.val
FROM pd_cross_a a
JOIN pd_cross_b b ON a.id = b.a_id
WHERE a.val > 10
ORDER BY a.id;

-- expected output:
2|20|25
3|30|5
Project
  Sort
    Hash Join
      Filter: (a.val > 10)
        Seq Scan on pd_cross_a
      Seq Scan on pd_cross_b

-- expected status: 0
