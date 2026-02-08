-- cross join
-- setup:
CREATE TABLE colors (name TEXT);
INSERT INTO colors (name) VALUES ('red'), ('blue');
CREATE TABLE sizes (sz TEXT);
INSERT INTO sizes (sz) VALUES ('S'), ('M');
-- input:
SELECT colors.name, sizes.sz FROM colors CROSS JOIN sizes ORDER BY colors.name, sizes.sz;
-- expected output:
blue|M
blue|S
red|M
red|S
-- expected status: 0
