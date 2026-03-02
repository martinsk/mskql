-- CTE body = JOIN+agg, outer = JOIN+GROUP BY+ORDER BY (mixed_analytical shape)
-- setup:
CREATE TABLE ma_orders (id INT, customer_id INT, product_id INT, quantity INT);
CREATE TABLE ma_products (id INT, price INT);
CREATE TABLE ma_customers (id INT, region TEXT);
INSERT INTO ma_orders VALUES (1, 1, 1, 2);
INSERT INTO ma_orders VALUES (2, 1, 2, 1);
INSERT INTO ma_orders VALUES (3, 2, 1, 3);
INSERT INTO ma_orders VALUES (4, 3, 2, 1);
INSERT INTO ma_products VALUES (1, 100);
INSERT INTO ma_products VALUES (2, 200);
INSERT INTO ma_customers VALUES (1, 'north');
INSERT INTO ma_customers VALUES (2, 'south');
INSERT INTO ma_customers VALUES (3, 'north');
-- input:
WITH order_summary AS (
  SELECT o.customer_id, SUM(o.quantity * p.price) AS total
  FROM ma_orders o JOIN ma_products p ON o.product_id = p.id
  GROUP BY o.customer_id
)
SELECT c.region, COUNT(*) AS num_customers, SUM(os.total) AS revenue
FROM order_summary os JOIN ma_customers c ON os.customer_id = c.id
GROUP BY c.region ORDER BY revenue DESC;
EXPLAIN WITH order_summary AS (
  SELECT o.customer_id, SUM(o.quantity * p.price) AS total
  FROM ma_orders o JOIN ma_products p ON o.product_id = p.id
  GROUP BY o.customer_id
)
SELECT c.region, COUNT(*) AS num_customers, SUM(os.total) AS revenue
FROM order_summary os JOIN ma_customers c ON os.customer_id = c.id
GROUP BY c.region ORDER BY revenue DESC
-- expected output:
north|2|600
south|1|300
Sort
  HashAggregate
    Hash Join
      Seq Scan on order_summary
      Seq Scan on ma_customers
