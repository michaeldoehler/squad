-- Data types: storage, formatting, casts and comparisons.
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE ty (
    id int PRIMARY KEY, i2 smallint, i8 bigint, f4 real, f8 double precision, n numeric(12,4), nn numeric,
    t text, vc varchar(5), c char(4), b bytea, bo boolean, d date, tm time, ts timestamp, tz timestamptz,
    iv interval, u uuid, j json, jb jsonb, ia int[], ta text[], m mood
);
INSERT INTO ty VALUES (1, 32767, 9223372036854775807, 1.5, 2.5e10, 1234.5678, 0.000123, 'héllo', 'abc', 'x', '\xdeadbeef', true,
    '2024-02-29', '13:14:15.123456', '2024-02-29 13:14:15', '2024-02-29 13:14:15+02', '1 year 2 mons 3 days 04:05:06',
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"a" : 1}', '{"b": 2, "a": [1, 2, {"c": null}]}', '{1,2,NULL}', '{"a b",c}', 'happy');
INSERT INTO ty (id) VALUES (2);
SELECT id, i2, i8, f4, f8, n, nn FROM ty ORDER BY id;
SELECT id, t, vc, c || '|', b, bo FROM ty ORDER BY id;
SELECT id, d, tm, ts, tz, iv FROM ty ORDER BY id;
SELECT id, u, j, jb, ia, ta, m FROM ty ORDER BY id;
SELECT pg_typeof(i2), pg_typeof(i8), pg_typeof(n), pg_typeof(t), pg_typeof(bo), pg_typeof(tz), pg_typeof(ia), pg_typeof(m) FROM ty WHERE id = 1;
-- integers and overflow
SELECT 32767::smallint + 1::smallint;
SELECT 9223372036854775807 + 1;
SELECT (-2147483648)::int / -1;
SELECT 1::smallint + 1::bigint, pg_typeof(1::smallint + 1::bigint), pg_typeof(1 + 1.5), pg_typeof(1 + 1.5::float4), pg_typeof(1::numeric + 1.5::float8);
SELECT 7 / 2, -7 / 2, 7 % -2, 7.5 % 2, abs(-3), mod(10, 3), div(7.5, 2);
-- numeric
SELECT 1.10 + 2.205, 1.5 * 2.25, 10 / 3.0, 1::numeric / 3, round(2.5), round(-2.5), round(3.14159, 2), trunc(-3.99), ceil(2.1), floor(-2.1);
SELECT 0.1 + 0.2 = 0.3, 0.1::float8 + 0.2::float8 = 0.3::float8, 1e-3::numeric, 123.456::numeric(5,1), 2 ^ 0.5;
SELECT 99999.99999::numeric(7,2);
SELECT 100000::numeric(7,2);
-- floats
SELECT 1.0::float8 / 3, 1e300::float8 * 10, 'NaN'::float8, 'Infinity'::float8, -'Infinity'::float8, 3::float4 / 7, 0.1::float4;
SELECT 1e300::float8 * 1e300::float8;
-- text and char
SELECT 'abc'::varchar(2), 'ab'::char(4) || '|', length('ab'::char(4)), 'a' = 'a  '::char(3);
INSERT INTO ty (id, vc) VALUES (3, 'toolong');
INSERT INTO ty (id, vc) VALUES (3, 'ok   ');
SELECT vc || '|' FROM ty WHERE id = 3;
-- booleans
SELECT 't'::boolean, 'no'::boolean, 'ON'::boolean, 0::boolean, true::int, true::text, NOT true, true > false;
SELECT 'maybe'::boolean;
-- dates and times
SELECT '2024-01-31'::date + 1, '2024-03-01'::date - 1, '2024-03-01'::date - '2023-03-01'::date, '2024-01-15'::date + interval '1 month 2 days';
SELECT '2024-02-30'::date;
SELECT timestamp '2024-01-01 10:00:00' + interval '90 minutes', timestamp '2024-03-10 00:00:00' - timestamp '2024-03-01 12:00:00';
SELECT extract(year FROM date '2024-07-04'), extract(dow FROM date '2024-07-04'), extract(epoch FROM timestamp '2024-01-01 00:00:00'), date_trunc('month', timestamp '2024-07-04 12:34:56');
SELECT '2024-06-01 12:00:00+00'::timestamptz AT TIME ZONE 'Europe/Berlin', '2024-06-01 12:00:00'::timestamp AT TIME ZONE 'America/New_York';
SELECT to_char(timestamp '2024-07-04 08:05:09', 'YYYY-MM-DD HH24:MI:SS'), to_char(date '2024-07-04', 'Dy, DD Mon YYYY'), to_char(1234.5, '9,999.00');
SELECT interval '1 day' * 3, interval '1 hour' / 2, interval '25 hours', justify_hours(interval '25 hours'), age(date '2024-03-01', date '2023-01-15');
SELECT date '2024-01-01' < timestamp '2024-01-01 00:00:01', '2024-01-01'::date = '2024-01-01 00:00:00'::timestamp;
-- uuid, bytea
SELECT 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid < 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid;
SELECT 'not-a-uuid'::uuid;
SELECT '\x00ff'::bytea, 'abc'::bytea, length('\x00ff'::bytea), encode('\x00ff'::bytea, 'base64'), decode('3q2+7w==', 'base64');
-- json
SELECT j -> 'a', j ->> 'a', jb -> 'a' -> 2, jb #>> '{a,0}', jb ? 'b', jb @> '{"b": 2}', jsonb_typeof(jb -> 'a') FROM ty WHERE id = 1;
SELECT jsonb_build_object('a', 1, 'b', ARRAY[1, 2]), json_build_array(1, 'x', NULL, true), to_jsonb('x'::text), jsonb_array_length('[1,2,3]');
SELECT '{"a":1}'::jsonb || '{"b":2}'::jsonb, '{"a":1,"b":2}'::jsonb - 'a', jsonb_set('{"a":{"b":1}}', '{a,b}', '2'), '[1,2]'::jsonb = '[1, 2]'::jsonb;
SELECT '{bad json'::jsonb;
-- arrays
SELECT ia[1], ia[3], ia[9], array_length(ia, 1), ta[1], cardinality(ta), 2 = ANY (ia), ia @> ARRAY[1], array_append(ia, 4), array_to_string(ta, '-') FROM ty WHERE id = 1;
SELECT ARRAY[1, 2] || ARRAY[3], ARRAY[1, 2] = ARRAY[1, 2], ARRAY[1, 2] < ARRAY[1, 3], '{}'::int[], array_remove(ARRAY[1, 2, 1], 1), ARRAY[]::text[], (ARRAY[1, 2, 3, 4])[2:3];
SELECT unnest(ARRAY[3, 1, 2]) AS x ORDER BY x;
SELECT * FROM unnest(ARRAY['a', 'b'], ARRAY[1, 2, 3]) AS u (s, n) ORDER BY n;
-- enums
SELECT m, m > 'ok', m::text || '!' FROM ty WHERE id = 1;
INSERT INTO ty (id, m) VALUES (4, 'sad'), (5, 'ok');
SELECT id, m FROM ty WHERE m IS NOT NULL ORDER BY m DESC, id;
SELECT id FROM ty WHERE m = 'ok';
INSERT INTO ty (id, m) VALUES (6, 'furious');
SELECT 'happy'::mood, 'sad'::mood < 'happy'::mood;
-- NULL handling in sorting and comparison
SELECT id FROM ty ORDER BY i2 NULLS FIRST, id LIMIT 3;
SELECT id FROM ty ORDER BY i2 DESC, id LIMIT 2;
SELECT NULL::int + 1, NULL || 'a', coalesce(NULL::text, 'x'), NULL IS NOT DISTINCT FROM NULL, (1, NULL) IS NULL, ROW(NULL, NULL) IS NULL;
