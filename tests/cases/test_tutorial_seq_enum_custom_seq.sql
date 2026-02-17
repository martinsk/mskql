-- tutorial: sequences & enums - custom sequence starting at 1000 (sequences-enums.html)
-- setup:
CREATE SEQUENCE invoice_seq START WITH 1000;
CREATE TABLE invoices (id INT PRIMARY KEY, customer TEXT NOT NULL, amount INT NOT NULL);
INSERT INTO invoices (id, customer, amount) VALUES (NEXTVAL('invoice_seq'), 'Acme Corp', 500), (NEXTVAL('invoice_seq'), 'Globex Inc', 1200), (NEXTVAL('invoice_seq'), 'Initech', 750);
-- input:
SELECT * FROM invoices ORDER BY id;
-- expected output:
1000|Acme Corp|500
1001|Globex Inc|1200
1002|Initech|750
-- expected status: 0
