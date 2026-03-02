-- OR predicate within single table can be pushed down
-- setup:
CREATE TABLE pd_or_items (id INT PRIMARY KEY, category TEXT, price INT);
INSERT INTO pd_or_items VALUES (1, 'electronics', 500);
INSERT INTO pd_or_items VALUES (2, 'books', 20);
INSERT INTO pd_or_items VALUES (3, 'electronics', 50);
INSERT INTO pd_or_items VALUES (4, 'clothing', 100);

CREATE TABLE pd_or_stock (id INT PRIMARY KEY, item_id INT, qty INT);
INSERT INTO pd_or_stock VALUES (1, 1, 10);
INSERT INTO pd_or_stock VALUES (2, 2, 50);
INSERT INTO pd_or_stock VALUES (3, 3, 30);
INSERT INTO pd_or_stock VALUES (4, 4, 5);
-- input:
SELECT i.id, i.category, s.qty
FROM pd_or_items i
JOIN pd_or_stock s ON i.id = s.item_id
WHERE (i.category = 'electronics' OR i.category = 'books')
ORDER BY i.id;
EXPLAIN SELECT i.id, i.category, s.qty
FROM pd_or_items i
JOIN pd_or_stock s ON i.id = s.item_id
WHERE (i.category = 'electronics' OR i.category = 'books')
ORDER BY i.id;

-- expected output:
1|electronics|10
2|books|50
3|electronics|30
Project
  Sort
    Hash Join
      Filter: ( = 0)
        Seq Scan on pd_or_items
      Seq Scan on pd_or_stock

-- expected status: 0
