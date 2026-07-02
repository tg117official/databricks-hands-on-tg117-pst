/* =========================================================
   MULTI-TABLE QUERY-BASED CDC DEMO
   Database: MySQL
   Purpose:
   - Multiple source tables
   - One metadata/bookmark table
   - PySpark can loop through all active tables
   ========================================================= */

CREATE DATABASE IF NOT EXISTS inventory;
USE inventory;

/* =========================================================
   CLEANUP FOR DEMO
   ========================================================= */
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS cdc_tracking;

/* =========================================================
   SOURCE TABLE 1: customers
   ========================================================= */
CREATE TABLE customers (
  customer_id   VARCHAR(50) PRIMARY KEY,
  first_name    VARCHAR(100) NOT NULL,
  last_name     VARCHAR(100) NOT NULL,
  email         VARCHAR(150) UNIQUE,
  address       VARCHAR(255),
  phone_number  VARCHAR(30),
  isDeleted     TINYINT(1) NOT NULL DEFAULT 0,
  lastmodified  TIMESTAMP NOT NULL
               DEFAULT CURRENT_TIMESTAMP
               ON UPDATE CURRENT_TIMESTAMP
);

/* =========================================================
   SOURCE TABLE 2: products
   ========================================================= */
CREATE TABLE products (
  product_id    VARCHAR(50) PRIMARY KEY,
  product_name  VARCHAR(150) NOT NULL,
  category      VARCHAR(100),
  price         DECIMAL(10,2) NOT NULL,
  isDeleted     TINYINT(1) NOT NULL DEFAULT 0,
  lastmodified  TIMESTAMP NOT NULL
               DEFAULT CURRENT_TIMESTAMP
               ON UPDATE CURRENT_TIMESTAMP
);

/* =========================================================
   SOURCE TABLE 3: orders
   ========================================================= */
CREATE TABLE orders (
  order_id      VARCHAR(50) PRIMARY KEY,
  customer_id   VARCHAR(50) NOT NULL,
  order_date    DATE NOT NULL,
  order_status  VARCHAR(50) NOT NULL,
  total_amount  DECIMAL(10,2) NOT NULL,
  isDeleted     TINYINT(1) NOT NULL DEFAULT 0,
  lastmodified  TIMESTAMP NOT NULL
               DEFAULT CURRENT_TIMESTAMP
               ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_orders_customers
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

/* =========================================================
   SOURCE TABLE 4: order_items
   ========================================================= */
CREATE TABLE order_items (
  order_item_id VARCHAR(50) PRIMARY KEY,
  order_id      VARCHAR(50) NOT NULL,
  product_id    VARCHAR(50) NOT NULL,
  quantity      INT NOT NULL,
  unit_price    DECIMAL(10,2) NOT NULL,
  isDeleted     TINYINT(1) NOT NULL DEFAULT 0,
  lastmodified  TIMESTAMP NOT NULL
               DEFAULT CURRENT_TIMESTAMP
               ON UPDATE CURRENT_TIMESTAMP,
  CONSTRAINT fk_order_items_orders
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
  CONSTRAINT fk_order_items_products
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

/* =========================================================
   CDC METADATA / BOOKMARK TABLE
   One row per source table.

   table_name      : source table name
   primary_key_col : primary key column, useful later for MERGE
   watermark_col   : CDC timestamp column
   last_watermark  : last successfully processed timestamp
   is_active       : 1 means PySpark should process this table
   ========================================================= */
CREATE TABLE cdc_tracking (
  table_name       VARCHAR(100) PRIMARY KEY,
  primary_key_col  VARCHAR(100) NOT NULL,
  watermark_col    VARCHAR(100) NOT NULL DEFAULT 'lastmodified',
  last_watermark   DATETIME NOT NULL DEFAULT '1970-01-01 00:00:00',
  is_active        TINYINT(1) NOT NULL DEFAULT 1,
  updated_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                   ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO cdc_tracking
(table_name, primary_key_col, watermark_col, last_watermark, is_active)
VALUES
('customers',   'customer_id',   'lastmodified', '1970-01-01 00:00:00', 1),
('products',    'product_id',    'lastmodified', '1970-01-01 00:00:00', 1),
('orders',      'order_id',      'lastmodified', '1970-01-01 00:00:00', 1),
('order_items', 'order_item_id', 'lastmodified', '1970-01-01 00:00:00', 1)
ON DUPLICATE KEY UPDATE table_name = table_name;

/* =========================================================
   RUN 01: INITIAL INSERTS
   Execute this before first PySpark run.
   ========================================================= */
INSERT INTO customers (customer_id, first_name, last_name, email, address, phone_number)
VALUES
('C001', 'Amit',  'Sharma', 'amit@example.com',  'Pune',   '9999990001'),
('C002', 'Neha',  'Patil',  'neha@example.com',  'Mumbai', '9999990002'),
('C003', 'Rohit', 'Joshi',  'rohit@example.com', 'Nagpur', '9999990003');

INSERT INTO products (product_id, product_name, category, price)
VALUES
('P001', 'Laptop',   'Electronics', 55000.00),
('P002', 'Keyboard', 'Electronics', 1500.00),
('P003', 'Mouse',    'Electronics', 800.00);

INSERT INTO orders (order_id, customer_id, order_date, order_status, total_amount)
VALUES
('O001', 'C001', '2026-07-01', 'PLACED', 56500.00),
('O002', 'C002', '2026-07-01', 'PLACED', 800.00);

INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price)
VALUES
('OI001', 'O001', 'P001', 1, 55000.00),
('OI002', 'O001', 'P002', 1, 1500.00),
('OI003', 'O002', 'P003', 1, 800.00);

/* =========================================================
   RUN 02: UPDATE / INSERT / SOFT DELETE
   Execute this after first PySpark run, then run PySpark again.
   =========================================================

UPDATE customers
SET address = 'Pune - Hinjewadi', phone_number = '8888881111'
WHERE customer_id = 'C001';

UPDATE products
SET price = 52000.00
WHERE product_id = 'P001';

INSERT INTO orders (order_id, customer_id, order_date, order_status, total_amount)
VALUES ('O003', 'C003', '2026-07-02', 'PLACED', 1500.00);

INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price)
VALUES ('OI004', 'O003', 'P002', 1, 1500.00);

UPDATE orders
SET order_status = 'CANCELLED', isDeleted = 1
WHERE order_id = 'O002';

*/

/* =========================================================
   VERIFICATION QUERIES
   ========================================================= */
SELECT * FROM customers;
SELECT * FROM products;
SELECT * FROM orders;
SELECT * FROM order_items;
SELECT * FROM cdc_tracking;
