-- Top-N Sort: basic ORDER BY ... LIMIT
CREATE TABLE top_n_t (id INT, score INT);
INSERT INTO top_n_t VALUES (1, 50), (2, 30), (3, 90), (4, 10), (5, 70), (6, 20), (7, 80), (8, 40), (9, 60), (10, 100);
SELECT id, score FROM top_n_t ORDER BY score DESC LIMIT 3;
-- expect: 10|100, 3|90, 7|80
SELECT id, score FROM top_n_t ORDER BY score ASC LIMIT 3;
-- expect: 4|10, 6|20, 2|30
SELECT id, score FROM top_n_t ORDER BY id DESC LIMIT 5;
-- expect: 10|100, 9|60, 8|40, 7|80, 6|20
DROP TABLE top_n_t;
