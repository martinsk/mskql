-- Top-N Sort: multi-column ORDER BY with LIMIT
CREATE TABLE top_n_mc (a INT, b INT, c TEXT);
INSERT INTO top_n_mc VALUES (1, 3, 'x'), (1, 1, 'y'), (2, 2, 'z'), (2, 1, 'w'), (1, 2, 'v');
SELECT a, b, c FROM top_n_mc ORDER BY a ASC, b DESC LIMIT 3;
-- expect: 1|3|x, 1|2|v, 1|1|y
SELECT a, b, c FROM top_n_mc ORDER BY a DESC, b ASC LIMIT 2;
-- expect: 2|1|w, 2|2|z
DROP TABLE top_n_mc;
