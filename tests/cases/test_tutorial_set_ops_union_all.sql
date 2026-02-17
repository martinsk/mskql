-- tutorial: set operations - UNION ALL with tags (set-operations.html)
-- setup:
CREATE TABLE warehouse_east (product_id INT PRIMARY KEY, product_name TEXT NOT NULL, quantity INT NOT NULL);
CREATE TABLE warehouse_west (product_id INT PRIMARY KEY, product_name TEXT NOT NULL, quantity INT NOT NULL);
INSERT INTO warehouse_east VALUES (1, 'Widget A', 100), (2, 'Widget B', 200), (3, 'Gadget X', 50), (4, 'Gadget Y', 75), (5, 'Gizmo Z', 30);
INSERT INTO warehouse_west VALUES (2, 'Widget B', 150), (3, 'Gadget X', 80), (5, 'Gizmo Z', 60), (6, 'Doohickey', 120), (7, 'Thingamajig', 90);
-- input:
SELECT product_name, quantity, warehouse FROM (SELECT product_name, quantity, 'east' AS warehouse FROM warehouse_east UNION ALL SELECT product_name, quantity, 'west' AS warehouse FROM warehouse_west) AS combined ORDER BY product_name, warehouse;
-- expected output:
Doohickey|120|west
Gadget X|50|east
Gadget X|80|west
Gadget Y|75|east
Gizmo Z|30|east
Gizmo Z|60|west
Thingamajig|90|west
Widget A|100|east
Widget B|200|east
Widget B|150|west
-- expected status: 0
