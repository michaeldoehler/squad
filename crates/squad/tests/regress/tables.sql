-- Tables, constraints and DML.
CREATE TABLE users (
    id bigint PRIMARY KEY,
    email text NOT NULL UNIQUE,
    name varchar(20),
    age int CHECK (age >= 0),
    active boolean NOT NULL DEFAULT true,
    created timestamp DEFAULT '2024-01-01 00:00:00'
);
INSERT INTO users (id, email, name, age) VALUES (1, 'ada@example.org', 'Ada', 36), (2, 'bob@example.org', 'Bob', 25);
INSERT INTO users (id, email) VALUES (3, 'cy@example.org') RETURNING *;
SELECT * FROM users ORDER BY id;
SELECT id, name FROM users WHERE age > 30;
SELECT count(*), count(age), sum(age), avg(age), min(name), max(name) FROM users;
-- constraint violations
INSERT INTO users (id, email) VALUES (1, 'dup@example.org');
INSERT INTO users (id, email) VALUES (4, 'ada@example.org');
INSERT INTO users (id, email) VALUES (4, NULL);
INSERT INTO users (id, email, age) VALUES (4, 'neg@example.org', -1);
INSERT INTO users (id, email, name) VALUES (4, 'long@example.org', 'a name that is far too long');
INSERT INTO users (id, email, age) VALUES (4, 'type@example.org', 'old');
INSERT INTO users (id, nosuch) VALUES (4, 1);
INSERT INTO users (id, email) VALUES (4, 'a', 'b');
SELECT count(*) FROM users;
-- update and delete
UPDATE users SET age = age + 1 WHERE id = 1 RETURNING id, age;
UPDATE users SET name = upper(name), active = false WHERE age IS NULL;
UPDATE users SET id = 2 WHERE id = 1;
UPDATE users SET email = 'bob@example.org' WHERE id = 1;
SELECT id, name, age, active FROM users ORDER BY id;
DELETE FROM users WHERE id = 3 RETURNING email;
DELETE FROM users WHERE id = 99;
SELECT count(*) FROM users;
-- upsert
INSERT INTO users (id, email, name) VALUES (1, 'ada@example.org', 'Ada L.') ON CONFLICT (id) DO UPDATE SET name = excluded.name RETURNING id, name;
INSERT INTO users (id, email, name) VALUES (2, 'x@example.org', 'X') ON CONFLICT DO NOTHING;
INSERT INTO users (id, email, name) VALUES (5, 'bob@example.org', 'X') ON CONFLICT (email) DO UPDATE SET name = users.name || '!' WHERE users.id = 2;
SELECT id, email, name FROM users ORDER BY id;
-- serial and defaults
CREATE TABLE items (id serial PRIMARY KEY, label text DEFAULT 'none', qty int DEFAULT 1 + 1);
INSERT INTO items DEFAULT VALUES;
INSERT INTO items (label) VALUES ('a'), ('b');
INSERT INTO items (label, qty) VALUES (DEFAULT, 7) RETURNING id, label, qty;
SELECT * FROM items ORDER BY id;
-- foreign keys
CREATE TABLE orders (id int PRIMARY KEY, user_id bigint NOT NULL REFERENCES users (id), item_id int REFERENCES items (id) ON DELETE SET NULL);
INSERT INTO orders VALUES (1, 1, 1), (2, 2, 2);
INSERT INTO orders VALUES (3, 99, 1);
DELETE FROM users WHERE id = 1;
DELETE FROM items WHERE id = 2;
SELECT * FROM orders ORDER BY id;
CREATE TABLE lines (order_id int REFERENCES orders (id) ON DELETE CASCADE, n int, PRIMARY KEY (order_id, n));
INSERT INTO lines VALUES (1, 1), (1, 2), (2, 1);
DELETE FROM orders WHERE id = 1;
SELECT * FROM lines ORDER BY 1, 2;
-- drop and truncate
DROP TABLE users;
TRUNCATE lines;
SELECT count(*) FROM lines;
DROP TABLE lines, orders;
DROP TABLE users;
DROP TABLE nosuch;
DROP TABLE IF EXISTS nosuch;
