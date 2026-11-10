-- Common table expressions.
CREATE TABLE tree (id int PRIMARY KEY, parent int, name text);
INSERT INTO tree VALUES (1, NULL, 'root'), (2, 1, 'a'), (3, 1, 'b'), (4, 2, 'a1'), (5, 2, 'a2'), (6, 4, 'a1x');
WITH x AS (SELECT 1 AS n) SELECT * FROM x;
WITH x AS (SELECT id, name FROM tree WHERE parent = 1), y AS (SELECT count(*) AS c FROM x) SELECT x.name, y.c FROM x, y ORDER BY 1;
WITH x (a, b) AS (SELECT id, name FROM tree) SELECT b FROM x WHERE a = 3;
WITH RECURSIVE n (i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 5) SELECT i FROM n;
WITH RECURSIVE n (i) AS (SELECT 1 UNION SELECT (i % 3) + 1 FROM n) SELECT i FROM n ORDER BY i;
WITH RECURSIVE sub AS (SELECT id, name, 0 AS depth FROM tree WHERE id = 2 UNION ALL SELECT t.id, t.name, sub.depth + 1 FROM tree t JOIN sub ON t.parent = sub.id) SELECT * FROM sub ORDER BY depth, id;
WITH RECURSIVE path AS (SELECT id, name::text AS p FROM tree WHERE parent IS NULL UNION ALL SELECT t.id, path.p || '/' || t.name FROM tree t JOIN path ON t.parent = path.id) SELECT p FROM path ORDER BY p;
WITH RECURSIVE fib (a, b) AS (SELECT 0, 1 UNION ALL SELECT b, a + b FROM fib WHERE b < 50) SELECT a FROM fib;
WITH a AS (SELECT 1 AS v), b AS (SELECT v + 1 AS v FROM a) SELECT a.v, b.v FROM a, b;
WITH x AS (SELECT 1 AS n) SELECT (SELECT n FROM x), EXISTS (SELECT 1 FROM x WHERE n = 2);
WITH ins AS (INSERT INTO tree VALUES (7, 3, 'b1') RETURNING id, name) SELECT * FROM ins;
WITH del AS (DELETE FROM tree WHERE id = 7 RETURNING name) SELECT count(*) FROM del;
WITH upd AS (UPDATE tree SET name = name || '!' WHERE parent = 4 RETURNING id) SELECT id FROM upd;
SELECT name FROM tree WHERE id = 6;
WITH x AS (SELECT 1) SELECT * FROM y;
WITH RECURSIVE n (i) AS (SELECT 1 UNION ALL SELECT 'x' FROM n) SELECT * FROM n;
