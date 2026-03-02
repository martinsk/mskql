-- plan: legacy exec outer filter with order by
-- setup:
CREATE TABLE lex_outer_t (id INT, score INT, label TEXT);
INSERT INTO lex_outer_t VALUES (1, 90, 'high'), (2, 40, 'low'), (3, 70, 'mid'), (4, 55, 'low'), (5, 80, 'high');
-- input:
SELECT id, score, label FROM lex_outer_t ORDER BY score DESC
EXPLAIN SELECT id, score, label FROM lex_outer_t ORDER BY score DESC
-- expected output:
1|90|high
5|80|high
3|70|mid
4|55|low
2|40|low
Project
  Sort (score DESC)
    Seq Scan on lex_outer_t
-- expected status: 0
