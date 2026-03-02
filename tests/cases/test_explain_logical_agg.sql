-- explain logical: aggregate with GROUP BY and ORDER BY
-- setup:
CREATE TABLE orders (id INT, customer TEXT, amount INT);
INSERT INTO orders VALUES (1,'alice',500),(2,'bob',200),(3,'alice',300);
-- input:
EXPLAIN (LOGICAL) SELECT customer, SUM(amount) FROM orders GROUP BY customer ORDER BY customer
-- expected output:
Sort [customer]
  Project [customer]
    Aggregate GROUP BY [customer]
      Scan on orders
-- expected status: 0
