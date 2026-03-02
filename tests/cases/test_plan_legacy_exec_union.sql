-- plan: legacy exec union set op
-- setup:
CREATE TABLE lex_union_a (id INT, val TEXT);
CREATE TABLE lex_union_b (id INT, val TEXT);
INSERT INTO lex_union_a VALUES (1, 'alpha'), (2, 'beta');
INSERT INTO lex_union_b VALUES (2, 'beta'), (3, 'gamma');
-- input:
SELECT id, val FROM lex_union_a UNION SELECT id, val FROM lex_union_b ORDER BY id
EXPLAIN SELECT id, val FROM lex_union_a UNION SELECT id, val FROM lex_union_b ORDER BY id
-- expected output:
1|alpha
2|beta
3|gamma
Sort
  HashSetOp Union
    Project
      Seq Scan on lex_union_a
    Project
      Seq Scan on lex_union_b
-- expected status: 0
