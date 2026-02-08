-- update from join
-- setup:
CREATE TABLE prices (id INT, price INT);
CREATE TABLE upd_src (id INT, new_price INT);
INSERT INTO prices (id, price) VALUES (1, 100), (2, 200);
INSERT INTO upd_src (id, new_price) VALUES (1, 150);
-- input:
UPDATE prices SET price = 150 FROM upd_src WHERE prices.id = upd_src.id;
SELECT id, price FROM prices ORDER BY id;
-- expected output:
UPDATE 1
1|150
2|200
-- expected status: 0
