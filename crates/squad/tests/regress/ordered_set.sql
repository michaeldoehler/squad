-- Ordered-set aggregates: percentile_cont, percentile_disc and mode
-- with WITHIN GROUP.
CREATE TABLE os (g int, x int, f float8, n numeric, t text, i interval);
INSERT INTO os VALUES (1, 1, 1.5, 1.5, 'b', '1 day'), (1, 2, 2.5, 2.5, 'a', '2 days'), (1, 4, NULL, 4, 'b', '1 hour'),
    (1, NULL, 10, NULL, NULL, NULL), (2, 7, 7, 7, 'z', '1 mon'), (3, NULL, NULL, NULL, NULL, NULL);
SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY x), percentile_cont(0.25) WITHIN GROUP (ORDER BY x DESC), percentile_disc(0.5) WITHIN GROUP (ORDER BY x), percentile_disc(0) WITHIN GROUP (ORDER BY t), mode() WITHIN GROUP (ORDER BY t), mode() WITHIN GROUP (ORDER BY t DESC) FROM os GROUP BY g ORDER BY g;
SELECT g, percentile_cont(0.5) WITHIN GROUP (ORDER BY i), percentile_disc(0.5) WITHIN GROUP (ORDER BY n), percentile_cont(0.3) WITHIN GROUP (ORDER BY n), percentile_cont(0.75) WITHIN GROUP (ORDER BY f) FROM os GROUP BY g ORDER BY g;
SELECT pg_typeof(percentile_cont(0.5) WITHIN GROUP (ORDER BY x)), pg_typeof(percentile_disc(0.5) WITHIN GROUP (ORDER BY x)), pg_typeof(percentile_cont(0.5) WITHIN GROUP (ORDER BY n)), pg_typeof(mode() WITHIN GROUP (ORDER BY n)), pg_typeof(percentile_cont(0.5) WITHIN GROUP (ORDER BY i)) FROM os;
SELECT percentile_cont(ARRAY[0, 0.5, 1, NULL]) WITHIN GROUP (ORDER BY x), percentile_disc(ARRAY[0.1, 0.9]) WITHIN GROUP (ORDER BY t), pg_typeof(percentile_disc(ARRAY[0.5]) WITHIN GROUP (ORDER BY x)) FROM os;
SELECT percentile_cont(ARRAY[]::float8[]) WITHIN GROUP (ORDER BY x), percentile_cont(ARRAY[2]) WITHIN GROUP (ORDER BY x) FROM os WHERE false;
SELECT percentile_cont(NULL) WITHIN GROUP (ORDER BY x), percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FILTER (WHERE g = 1) FROM os;
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) AS p, sum(x) FROM os WHERE false;
SELECT g, percentile_cont(g * 0.1) WITHIN GROUP (ORDER BY x) FROM os GROUP BY g ORDER BY g;
SELECT g FROM os GROUP BY g HAVING percentile_cont(0.5) WITHIN GROUP (ORDER BY x) > 2 ORDER BY g;
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) FROM os ORDER BY percentile_cont(0.5) WITHIN GROUP (ORDER BY x);
SELECT mode() WITHIN GROUP (ORDER BY x) FROM (VALUES (1), (2), (2), (3), (3)) v(x);
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY x), percentile_disc(0.51) WITHIN GROUP (ORDER BY x), percentile_cont(1) WITHIN GROUP (ORDER BY x::float4) FROM (VALUES (1), (2), (3), (4)) v(x);
SELECT percentile_cont(0.1) WITHIN GROUP (ORDER BY x) FROM (VALUES (1::bigint), (9223372036854775807)) v(x);
SELECT percentile_cont(0.333) WITHIN GROUP (ORDER BY x) FROM (VALUES ('1 year'::interval), ('1 day 3 hours')) v(x);
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY '1') FROM os;
SELECT percentile_cont('0.5') WITHIN GROUP (ORDER BY x::numeric), percentile_disc(0.5) WITHIN GROUP (ORDER BY NULL::int) FROM (VALUES (1), (2)) v(x);
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x), percentile_disc(0.5) WITHIN GROUP (ORDER BY x) FROM (VALUES (1::numeric), (2.5)) v(x);
SELECT percentile_disc(0.5) WITHIN GROUP (ORDER BY f NULLS FIRST), percentile_disc(0) WITHIN GROUP (ORDER BY f DESC NULLS FIRST) FROM os;
-- errors
SELECT percentile_cont(1.5) WITHIN GROUP (ORDER BY x) FROM os;
SELECT percentile_cont(1.5) WITHIN GROUP (ORDER BY x) FROM os WHERE false;
SELECT percentile_cont(-0.1) WITHIN GROUP (ORDER BY x) FROM os WHERE g = 3;
SELECT percentile_cont('nan') WITHIN GROUP (ORDER BY x) FROM os;
SELECT percentile_cont(ARRAY[0.5, 2]) WITHIN GROUP (ORDER BY x) FROM os;
SELECT percentile_cont(x) WITHIN GROUP (ORDER BY x) FROM os;
SELECT percentile_cont(avg(x) / 10) WITHIN GROUP (ORDER BY x) FROM os;
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY sum(x)) FROM os;
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY t) FROM os;
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x, f) FROM os;
SELECT mode(x) WITHIN GROUP (ORDER BY x) FROM os;
SELECT mode() WITHIN GROUP (ORDER BY NULL) FROM os;
SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x) OVER () FROM os;
SELECT percentile_cont(0.5) FROM os;
SELECT count(*) WITHIN GROUP (ORDER BY x) FROM os;
SELECT sum(x) WITHIN GROUP (ORDER BY x) FROM os;
SELECT x FROM os WHERE percentile_cont(0.5) WITHIN GROUP (ORDER BY x) > 1;
