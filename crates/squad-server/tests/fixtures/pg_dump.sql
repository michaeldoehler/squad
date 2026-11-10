--
-- PostgreSQL database dump
--

\restrict TqqI2FEO1wjPi42XGtgH6Jxg47Da3QyK45D0aZYpx96dHp9Ucb0oeHQ7ec3EyL3

-- Dumped from database version 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)
-- Dumped by pg_dump version 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: mood; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.mood AS ENUM (
    'sad',
    'ok',
    'happy'
);


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: authors; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.authors (
    id integer NOT NULL,
    name text NOT NULL,
    mood public.mood DEFAULT 'ok'::public.mood,
    born date,
    meta jsonb
);


--
-- Name: books; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.books (
    id bigint NOT NULL,
    author_id integer NOT NULL,
    title character varying(200) NOT NULL,
    price numeric(10,2),
    tags text[],
    published timestamp with time zone,
    blob bytea,
    CONSTRAINT books_price_check CHECK ((price >= (0)::numeric))
);


--
-- Name: author_books; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.author_books AS
 SELECT a.name,
    count(b.id) AS n
   FROM (public.authors a
     LEFT JOIN public.books b ON ((b.author_id = a.id)))
  GROUP BY a.name;


--
-- Name: authors_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.authors_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: authors_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.authors_id_seq OWNED BY public.authors.id;


--
-- Name: books_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.books_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: books_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.books_id_seq OWNED BY public.books.id;


--
-- Name: authors id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.authors ALTER COLUMN id SET DEFAULT nextval('public.authors_id_seq'::regclass);


--
-- Name: books id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.books ALTER COLUMN id SET DEFAULT nextval('public.books_id_seq'::regclass);


--
-- Data for Name: authors; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.authors (id, name, mood, born, meta) FROM stdin;
1	Ada	happy	1815-12-10	{"k": [1, 2]}
2	Tab\tBob	sad	\N	\N
3	New\nLine\\	ok	2000-01-01	{}
\.


--
-- Data for Name: books; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.books (id, author_id, title, price, tags, published, blob) FROM stdin;
1	1	Notes	12.50	{math,"with space"}	2024-01-02 03:04:05+00	\\x00ff10
2	2	Empty	\N	\N	\N	\N
\.


--
-- Name: authors_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.authors_id_seq', 3, true);


--
-- Name: books_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.books_id_seq', 2, true);


--
-- Name: authors authors_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.authors
    ADD CONSTRAINT authors_name_key UNIQUE (name);


--
-- Name: authors authors_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.authors
    ADD CONSTRAINT authors_pkey PRIMARY KEY (id);


--
-- Name: books books_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.books
    ADD CONSTRAINT books_pkey PRIMARY KEY (id);


--
-- Name: books_author_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX books_author_idx ON public.books USING btree (author_id);


--
-- Name: books books_author_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.books
    ADD CONSTRAINT books_author_id_fkey FOREIGN KEY (author_id) REFERENCES public.authors(id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict TqqI2FEO1wjPi42XGtgH6Jxg47Da3QyK45D0aZYpx96dHp9Ucb0oeHQ7ec3EyL3

