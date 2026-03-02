-- plan: order by expression alias
-- setup:
CREATE TABLE txns (id INT, amount INT, fee INT, category TEXT);
INSERT INTO txns VALUES (1, 10000, 200, 'dining');
INSERT INTO txns VALUES (2, 8000, 150, 'dining');
INSERT INTO txns VALUES (3, 3000, 50, 'dining');
INSERT INTO txns VALUES (4, 6000, 100, 'travel');
INSERT INTO txns VALUES (5, 9000, 180, 'dining');
-- input:
SELECT amount - fee AS net FROM txns WHERE category = 'dining' AND amount > 5000 ORDER BY net DESC
EXPLAIN SELECT amount - fee AS net FROM txns WHERE category = 'dining' AND amount > 5000 ORDER BY net DESC
-- expected output:
9800
8820
7850
Sort
  Vec Project
    Filter: (amount > 5000)
      Filter: (category = 'dining')
        Seq Scan on txns
-- expected status: 0
