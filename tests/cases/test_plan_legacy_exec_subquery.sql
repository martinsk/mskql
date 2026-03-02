-- plan: legacy exec subquery simple order by
-- setup:
CREATE TABLE legacy_sub_t (id INT, val INT, name TEXT);
INSERT INTO legacy_sub_t VALUES (1, 10, 'alice'), (2, 20, 'bob'), (3, 30, 'carol');
-- input:
SELECT id, val, name FROM legacy_sub_t ORDER BY id
EXPLAIN SELECT id, val, name FROM legacy_sub_t ORDER BY id
-- expected output:
1|10|alice
2|20|bob
3|30|carol
Project
  Sort (id)
    Seq Scan on legacy_sub_t
-- expected status: 0
