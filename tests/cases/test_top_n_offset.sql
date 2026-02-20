-- Top-N Sort: ORDER BY ... LIMIT ... OFFSET
CREATE TABLE top_n_off (id INT, val INT);
INSERT INTO top_n_off VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60), (7, 70), (8, 80);
SELECT id, val FROM top_n_off ORDER BY val ASC LIMIT 3 OFFSET 2;
-- expect: 3|30, 4|40, 5|50
SELECT id, val FROM top_n_off ORDER BY val DESC LIMIT 2 OFFSET 1;
-- expect: 7|70, 6|60
SELECT id, val FROM top_n_off ORDER BY id LIMIT 3 OFFSET 5;
-- expect: 6|60, 7|70, 8|80
DROP TABLE top_n_off;
