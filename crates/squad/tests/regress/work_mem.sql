-- work_mem: the setting, and queries whose sorts, hash tables and window
-- partitions exceed it, over data with NULLs, duplicates and many types.
SHOW work_mem;
SET work_mem = '64kB';
SHOW work_mem;
SET work_mem = 2048;
SHOW work_mem;
SET work_mem = '1.5MB';
SHOW work_mem;
SET work_mem = '1GB';
SHOW work_mem;
SET work_mem = '63kB';
SET work_mem = '4 mb';
SET work_mem = 'lots';
SELECT current_setting('work_mem');
RESET work_mem;
SHOW work_mem;
BEGIN;
SET LOCAL work_mem = '128kB';
SHOW work_mem;
COMMIT;
SHOW work_mem;
BEGIN;
SET work_mem = '256kB';
ROLLBACK;
SHOW work_mem;
SELECT set_config('work_mem', '8MB', false);
SHOW work_mem;
CREATE TABLE w (id int PRIMARY KEY, k int, txt text, num numeric, j jsonb, arr int[], ts timestamp);
INSERT INTO w SELECT g,
    CASE WHEN g % 17 = 0 THEN NULL ELSE g % 500 END,
    CASE WHEN g % 13 = 0 THEN NULL ELSE 'v' || (g * 7919 % 1000) END,
    CASE WHEN g % 11 = 0 THEN NULL ELSE (g % 97) / 7.0 END,
    CASE WHEN g % 19 = 0 THEN NULL ELSE jsonb_build_object('a', g % 50, 'b', 'x' || (g % 3)) END,
    CASE WHEN g % 23 = 0 THEN NULL ELSE ARRAY[g % 5, g % 7] END,
    timestamp '2020-01-01' + (g % 1000) * interval '1 hour'
  FROM generate_series(1, 20000) g;
SET work_mem = '64kB';
-- Sorting
SELECT id, txt, num FROM w ORDER BY txt NULLS FIRST, num DESC, id OFFSET 9990 LIMIT 15;
SELECT id, txt, num FROM w ORDER BY txt DESC, num NULLS FIRST, id OFFSET 19990;
SELECT id, arr FROM w ORDER BY arr DESC NULLS LAST, id LIMIT 10 OFFSET 12000;
SELECT ts, id FROM w ORDER BY ts DESC, id LIMIT 5 OFFSET 7000;
SELECT DISTINCT ON (k) k, id, txt FROM w ORDER BY k, txt DESC, id LIMIT 5 OFFSET 300;
-- Aggregation
SELECT k, count(*), sum(num), min(txt), max(ts) FROM w GROUP BY k ORDER BY k NULLS FIRST LIMIT 10 OFFSET 200;
SELECT count(*), sum(c), max(c) FROM (SELECT txt, ts, count(*) c FROM w GROUP BY txt, ts) s;
SELECT id % 400 AS g, count(*), string_agg(coalesce(txt, '-'), ',' ORDER BY id), array_agg(num ORDER BY id), count(DISTINCT txt)
  FROM w GROUP BY 1 ORDER BY 1 LIMIT 3 OFFSET 100;
SELECT count(*), sum(n) FROM (SELECT j, arr, count(*) n FROM w GROUP BY j, arr) s;
SELECT count(*), count(DISTINCT txt) FROM (SELECT DISTINCT txt, arr FROM w) s;
SELECT count(*) FROM (SELECT DISTINCT j, num FROM w) s;
-- Joins
SELECT count(*), sum(a.id::bigint * b.id) FROM w a JOIN w b ON a.txt = b.txt AND a.id < b.id;
SELECT count(*), sum(a.id), count(b.id) FROM w a LEFT JOIN (SELECT * FROM w WHERE id % 10 = 0) b ON a.k = b.k;
SELECT count(*), count(a.id), count(b.id) FROM (SELECT * FROM w WHERE id % 3 = 0) a RIGHT JOIN (SELECT * FROM w WHERE id % 5 = 0) b ON a.num = b.num;
SELECT count(*), count(a.id), count(b.id) FROM (SELECT * FROM w WHERE id % 3 = 0) a FULL JOIN (SELECT * FROM w WHERE id % 7 = 0) b ON a.k = b.k;
SELECT count(*) FROM w WHERE txt IN (SELECT txt FROM w WHERE id % 7 = 0);
SELECT count(*) FROM w a WHERE a.id % 50 = 0 AND NOT EXISTS (SELECT 1 FROM w b WHERE b.k = a.k AND b.id % 2 = 0);
-- Set operations
SELECT count(*) FROM (SELECT txt, arr FROM w INTERSECT SELECT txt, arr FROM w WHERE id % 2 = 0) s;
SELECT count(*) FROM (SELECT txt, k FROM w INTERSECT ALL SELECT txt, k FROM w WHERE id % 2 = 0) s;
SELECT count(*) FROM (SELECT txt, ts FROM w EXCEPT SELECT txt, ts FROM w WHERE id % 3 = 0) s;
SELECT count(*) FROM (SELECT k, num FROM w EXCEPT ALL SELECT k, num FROM w WHERE id % 3 = 0) s;
SELECT count(*) FROM (SELECT txt, num FROM w UNION SELECT txt, k FROM w) s;
-- Window functions and CTEs
SELECT id, row_number() OVER (PARTITION BY id % 500 ORDER BY txt, id), sum(num) OVER (PARTITION BY id / 40 ORDER BY id)
  FROM w ORDER BY id LIMIT 5 OFFSET 10000;
WITH c AS (SELECT * FROM w) SELECT count(*), sum(x.id) FROM c x JOIN c y ON x.id = y.id + 1;
WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 30000) SELECT count(*), sum(n) FROM r;
RESET work_mem;
