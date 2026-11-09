-- Differences found by the differential fuzzer (squad-difftest).
CREATE TABLE f (id int PRIMARY KEY, n numeric(9,2), s varchar(6), j jsonb, u uuid, b boolean, ref int REFERENCES f (id));
INSERT INTO f VALUES (1, 6.04, 'abc', '"s"', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', true, NULL), (2, NULL, NULL, '{"a": 1}', NULL, false, 1);
-- numeric addition keeps the larger scale even when one operand is zero
SELECT 0.00000000000000000000 + 28.2068, (0::numeric / 1) + 1.5, 1.5 - 0.000, (trunc(0.72) / 1) + (n * (-4.67)) FROM f WHERE id = 1;
-- min/max are not defined for every comparable type
SELECT max(u) FROM f;
SELECT min(b) FROM f;
SELECT max('\x00'::bytea);
-- polymorphic window functions need a typed argument
SELECT lead('x') OVER (ORDER BY id) FROM f;
SELECT first_value(NULL) OVER (ORDER BY id) FROM f;
SELECT lag('x'::text) OVER (ORDER BY id) FROM f ORDER BY id;
-- LATERAL inside a join may reference items to the left of the join; plain join conditions may not
SELECT a.id, l.v FROM f a, f b LEFT JOIN LATERAL (SELECT max(x.id) AS v FROM f x WHERE x.id <= a.id) l ON true ORDER BY 1, 2;
SELECT a.id FROM f a, f b JOIN f c ON a.id = c.id;
SELECT a.id FROM f a, f b LEFT JOIN f c ON c.id = b.id AND EXISTS (SELECT 1 FROM f d WHERE d.id = a.id);
-- element 0 of a jsonb scalar is the scalar itself
SELECT j -> 0, j -> -1, j -> 1, '"s"'::json -> 0, '3'::jsonb -> 0 -> 0 FROM f ORDER BY id;
-- a constant is not a sort key
SELECT id FROM f ORDER BY 'x';
SELECT id FROM f ORDER BY 1.5;
SELECT id FROM f UNION SELECT 3 ORDER BY 'x';
-- RIGHT and FULL joins without an equality condition, with an empty left side
SELECT a.id, b.id FROM (SELECT id FROM f WHERE false) a RIGHT JOIN f b ON a.id < b.id ORDER BY 2;
WITH q (c0) AS (SELECT id FROM f WHERE false) SELECT q.c0, b.id FROM q RIGHT JOIN f b ON b.id > 0 ORDER BY 2;
-- ON CONFLICT DO UPDATE may not touch a row twice
INSERT INTO f (id, s) VALUES (9, 'a'), (9, 'b') ON CONFLICT (id) DO UPDATE SET s = excluded.s;
INSERT INTO f (id, s) VALUES (1, 'a'), (1, 'b') ON CONFLICT (id) DO UPDATE SET s = excluded.s WHERE f.id > 5;
INSERT INTO f (id, s) VALUES (9, 'a'), (9, 'b') ON CONFLICT (id) DO NOTHING;
-- constant assignments are checked even when no row is touched
UPDATE f SET s = 'far too long' WHERE false;
UPDATE f SET n = 12345678.9 WHERE false;
SELECT 'abcdefgh'::varchar(3), 1.239::numeric(3,2);
-- SET expressions see the table as of the start of the statement
UPDATE f SET n = (SELECT count(*) FROM f x WHERE x.n IS NULL) + id;
SELECT id, n FROM f ORDER BY id;
UPDATE f SET n = coalesce(n + 1, (SELECT min(x.n) FROM f x)) RETURNING id, n;
-- foreign keys are checked at the end of the statement
INSERT INTO f (id, ref) VALUES (21, 20), (20, NULL);
INSERT INTO f (id, ref) VALUES (30, 99), (31, NULL), (NULL, NULL);
INSERT INTO f (id, ref) VALUES (30, 99);
SELECT id, ref FROM f WHERE id >= 20 ORDER BY id;
-- nothing but transaction control is analyzed in an aborted transaction
BEGIN;
SELECT 1 / 0;
SELECT * FROM nosuchtable;
SELECT nosuchfunction();
ROLLBACK;
-- an error while analyzing a statement aborts the transaction too
BEGIN;
SELECT * FROM nosuchtable;
SELECT 1;
ROLLBACK;
BEGIN;
SELEC 1;
SELECT 1;
ROLLBACK;
-- constant subexpressions are evaluated while planning: errors surface without rows, except in branches constants rule out
SELECT power(-8::float8, 0.5) FROM f WHERE false;
SELECT 1 FROM f WHERE false AND power(-8::float8, 0.5) > 1;
SELECT CASE WHEN 1 IN (1, 2) THEN 'x' ELSE (sqrt(-1::numeric))::text END FROM f WHERE false;
SELECT CASE WHEN id > 0 THEN 'x' ELSE (sqrt(-1::numeric))::text END FROM f WHERE false;
SELECT coalesce(1, ln(0::numeric)::int), coalesce(NULL, ln(0::numeric)::int);
SELECT 1 FROM (SELECT power(-8::float8, 0.5) AS c FROM f WHERE false) d;
SELECT EXISTS (SELECT power(-8::float8, 0.5) FROM f WHERE false);
SELECT 2 + 3 * 4, upper('abc') || 'd', 10 / 4, 'a' < 'b', length(repeat('ab', 3));
-- path edits of jsonb scalars
SELECT jsonb_set('3'::jsonb, '{a}', '1');
SELECT jsonb_insert('"s"'::jsonb, '{0}', '1');
SELECT '3'::jsonb #- '{a}';
SELECT jsonb_set('[1]'::jsonb, '{0}', '2'), jsonb_set('{"a": 1}'::jsonb, '{b}', '2');
-- DISTINCT aggregates may only be ordered by their arguments
SELECT string_agg(DISTINCT s, ',' ORDER BY id) FROM f;
SELECT string_agg(DISTINCT s, ',' ORDER BY s) FROM f;
-- a CASE is named after its ELSE branch when that has a name
SELECT CASE WHEN id > 1 THEN 'x' ELSE s END, CASE WHEN id > 1 THEN 1 ELSE length(s) END, CASE WHEN id > 1 THEN 1 ELSE id::bigint END, CASE WHEN id > 1 THEN 1 ELSE 2 END, CASE WHEN id > 1 THEN 1 END FROM f WHERE false;
-- variance and standard deviation are exact for integer and numeric input
SELECT variance(x), stddev(x), var_pop(x), stddev_pop(x) FROM (VALUES (1), (2), (4), (1073741823)) v (x);
SELECT variance(x), stddev_pop(x), stddev(y), var_samp(y), stddev_pop(y) FROM (VALUES (5, 1.5), (7, 2.25), (8, 10.125)) v (x, y);
SELECT stddev(x), variance(x), stddev_pop(x), var_pop(x) FROM (VALUES (5)) v (x);
-- a constant TRUE in an OR (FALSE in an AND) decides it without evaluating the rest
SELECT id FROM f WHERE id / 0 > 1 OR 2 > 1 ORDER BY id LIMIT 1;
SELECT id FROM f WHERE id / 0 > 1 AND 1 > 2;
SELECT id FROM f WHERE id / 0 > 1 OR 1 > 2;
-- UPDATE reports the first failing row's error
CREATE TABLE u (id int PRIMARY KEY, a int NOT NULL, b smallint);
INSERT INTO u VALUES (1, 1, 1), (2, 100000, 2);
UPDATE u SET a = NULL, b = a;
UPDATE u SET b = a WHERE id = 2;
-- correlated subqueries in the ON clause of a join that is not the first FROM item
SELECT a.id, c.id FROM f a, f b JOIN f c ON (1 <> ANY (SELECT d.id FROM f d WHERE c.b)) WHERE a.id = 1 ORDER BY 1, 2;
SELECT a.id, b.id, c.id FROM f a, f b JOIN f c ON EXISTS (SELECT 1 FROM f d WHERE d.id = b.id AND c.id = 2) WHERE a.id <= 2 AND b.id <= 2 ORDER BY 1, 2, 3;
SELECT count(*) FROM f a, f b JOIN f c ON (b.n = ALL (SELECT d.n FROM f d WHERE b.b)) WHERE a.id = 1;
