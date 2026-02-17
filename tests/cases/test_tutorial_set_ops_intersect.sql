-- tutorial: set operations - INTERSECT (set-operations.html)
-- setup:
CREATE TABLE warehouse_east (product_id INT PRIMARY KEY, product_name TEXT NOT NULL, quantity INT NOT NULL);
CREATE TABLE warehouse_west (product_id INT PRIMARY KEY, product_name TEXT NOT NULL, quantity INT NOT NULL);
INSERT INTO warehouse_east VALUES (1, 'Widget A', 100), (2, 'Widget B', 200), (3, 'Gadget X', 50), (4, 'Gadget Y', 75), (5, 'Gizmo Z', 30);
INSERT INTO warehouse_west VALUES (2, 'Widget B', 150), (3, 'Gadget X', 80), (5, 'Gizmo Z', 60), (6, 'Doohickey', 120), (7, 'Thingamajig', 90);
-- input:
SELECT product_id, product_name FROM warehouse_east INTERSECT SELECT product_id, product_name FROM warehouse_west ORDER BY product_id;
-- expected output:
2|Widget B
3|Gadget X
5|Gizmo Z
-- expected status: 0
