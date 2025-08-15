--
-- PostgreSQL database dump
--

\restrict 2dIGo6x7nl11sPODtWm9oaFYA9PkoVHdGYwnbAnW9XfdxHe4Sb3qOZzr6j7HPvc

-- Dumped from database version 17.5
-- Dumped by pg_dump version 17.6

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: btree_gin; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS btree_gin WITH SCHEMA public;


--
-- Name: EXTENSION btree_gin; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION btree_gin IS 'support for indexing common datatypes in GIN';


--
-- Name: pg_trgm; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public;


--
-- Name: EXTENSION pg_trgm; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pg_trgm IS 'text similarity measurement and index searching based on trigrams';


--
-- Name: parse_flex_date(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.parse_flex_date(t text) RETURNS date
    LANGUAGE sql IMMUTABLE
    AS $_$
    SELECT CASE
        WHEN t IS NULL OR btrim(t) = '' THEN NULL
    WHEN t ~ '^\d{4}-\d{2}-\d{2}$' THEN t::date
    WHEN t ~ '^\d{4}-\d{2}-\d{2}[ T]' THEN left(t,10)::date
    WHEN t ~ '^\d{4}/\d{2}/\d{2}$' THEN to_date(t,'YYYY/MM/DD')
    WHEN t ~ '^\d{8}$' THEN to_date(t,'YYYYMMDD')
    WHEN t ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN to_date(t,'MM/DD/YY')
    WHEN t ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(t,'MM/DD/YYYY')
    WHEN t ~ '^\d{1,2}/\d{1,2}/\d{4} ' THEN to_date(split_part(t,' ',1),'MM/DD/YYYY')
    WHEN t ~ '^\d{1,2}/\d{1,2}/\d{2} ' THEN to_date(split_part(t,' ',1),'MM/DD/YY')
    WHEN t ~ '^\d{4}-\d{2}-\d{2}T' THEN left(t,10)::date
    WHEN t ~ '^\d{4}-\d{2}$' THEN (to_date(t||'-01','YYYY-MM-DD') + INTERVAL '1 month' - INTERVAL '1 day')::date
    WHEN t ~ '^\d{4}$' THEN to_date(t||'-12-31','YYYY-MM-DD')
        ELSE NULL
    END
$_$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: facts_provenance; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.facts_provenance (
    prov_id bigint NOT NULL,
    entity_table text NOT NULL,
    entity_pk bigint NOT NULL,
    field_name text NOT NULL,
    source_url text NOT NULL,
    xpath_hint text,
    quote_snippet text,
    ingested_at timestamp with time zone DEFAULT now()
);


--
-- Name: facts_provenance_prov_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.facts_provenance_prov_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: facts_provenance_prov_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.facts_provenance_prov_id_seq OWNED BY public.facts_provenance.prov_id;


--
-- Name: grants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.grants (
    grant_id bigint NOT NULL,
    return_id bigint,
    funder_org_id bigint,
    recipient_org_id bigint,
    recipient_name_raw text NOT NULL,
    recipient_name_line1 text,
    recipient_name_line2 text,
    recipient_city text,
    recipient_state character(2),
    recipient_zip text,
    recipient_country text,
    recipient_province text,
    recipient_postal text,
    amount_cash integer,
    amount_noncash integer,
    amount_total integer,
    purpose_text text,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: grants_grant_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.grants_grant_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: grants_grant_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.grants_grant_id_seq OWNED BY public.grants.grant_id;


--
-- Name: organizations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.organizations (
    org_id bigint NOT NULL,
    ein text,
    name text NOT NULL,
    aka text,
    ntee_major text,
    ntee_refined text,
    org_type text,
    is_foundation boolean DEFAULT false NOT NULL,
    address_line1 text,
    city text,
    state character(2),
    zip_code text,
    country text DEFAULT 'US'::text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    ntee_code text,
    ntee_major_name text,
    ntee_category_name text,
    ntee_source text,
    ntee_conflict boolean DEFAULT false,
    ntee_updated_at timestamp with time zone DEFAULT now(),
    ntee_tags text[] DEFAULT '{}'::text[],
    CONSTRAINT organizations_org_type_check CHECK ((org_type = ANY (ARRAY['PUBLIC_CHARITY'::text, 'PRIVATE_FOUNDATION'::text, 'OTHER'::text])))
);


--
-- Name: organizations_org_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.organizations_org_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: organizations_org_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.organizations_org_id_seq OWNED BY public.organizations.org_id;


--
-- Name: pf_payouts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.pf_payouts (
    return_id bigint NOT NULL,
    distributable_amount integer,
    qualifying_distributions integer,
    undistributed_income integer,
    payout_shortfall integer,
    payout_pressure_index numeric,
    fy_end_year integer,
    fy_end_month integer,
    computed_at timestamp with time zone DEFAULT now()
);


--
-- Name: returns; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.returns (
    return_id bigint NOT NULL,
    org_id bigint,
    tax_year integer,
    period_begin date,
    period_end date,
    form_type text,
    index_year integer,
    object_id text,
    source_url text NOT NULL,
    downloaded_at timestamp with time zone,
    CONSTRAINT returns_form_type_check CHECK ((form_type = ANY (ARRAY['F990'::text, 'F990PF'::text, 'F990T'::text, 'OTHER'::text])))
);


--
-- Name: returns_return_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.returns_return_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: returns_return_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.returns_return_id_seq OWNED BY public.returns.return_id;


--
-- Name: stg_filer; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_filer (
    ein text,
    organizationname text,
    addressline1 text,
    city text,
    state text,
    zipcode text,
    returntype text,
    taxperiodbegin text,
    taxperiodend text,
    taxyear text,
    businessofficer text,
    officertitle text,
    officerphone text,
    organization501ctype text,
    totalrevenue text,
    totalexpenses text,
    netassets text
);


--
-- Name: stg_grants; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_grants (
    filerein text,
    taxperiodend text,
    recipientname text,
    recipientnameline1 text,
    recipientnameline2 text,
    recipientcity text,
    recipientstate text,
    recipientzip text,
    recipientcountry text,
    recipientprovince text,
    recipientpostal text,
    grantamountcash text,
    grantamountnoncash text,
    grantamounttotal text,
    grantpurpose text
);


--
-- Name: stg_index; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_index (
    ein text,
    taxperiodend text,
    index_year text,
    object_id text,
    url text,
    formtype text
);


--
-- Name: stg_irs_bmf_raw; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_irs_bmf_raw (
    ein text,
    legal_name text,
    ntee_cd text,
    data jsonb,
    source_file text,
    bmf_loaded_at timestamp with time zone DEFAULT now()
);


--
-- Name: stg_nccs_pf_raw; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_nccs_pf_raw (
    ein text,
    legal_name text,
    nteecc text,
    nteefinal text,
    data jsonb,
    source_file text,
    nccs_loaded_at timestamp with time zone DEFAULT now()
);


--
-- Name: stg_pf_payout; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.stg_pf_payout (
    ein text,
    filername text,
    taxperiodend text,
    fyendyear text,
    fyendmonth text,
    distributableamount text,
    qualifyingdistributions text,
    undistributedincome text,
    payoutshortfall text,
    payoutpressureindex text
);


--
-- Name: facts_provenance prov_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.facts_provenance ALTER COLUMN prov_id SET DEFAULT nextval('public.facts_provenance_prov_id_seq'::regclass);


--
-- Name: grants grant_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.grants ALTER COLUMN grant_id SET DEFAULT nextval('public.grants_grant_id_seq'::regclass);


--
-- Name: organizations org_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organizations ALTER COLUMN org_id SET DEFAULT nextval('public.organizations_org_id_seq'::regclass);


--
-- Name: returns return_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.returns ALTER COLUMN return_id SET DEFAULT nextval('public.returns_return_id_seq'::regclass);


--
-- Name: facts_provenance facts_provenance_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.facts_provenance
    ADD CONSTRAINT facts_provenance_pkey PRIMARY KEY (prov_id);


--
-- Name: grants grants_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.grants
    ADD CONSTRAINT grants_pkey PRIMARY KEY (grant_id);


--
-- Name: organizations organizations_ein_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_ein_key UNIQUE (ein);


--
-- Name: organizations organizations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.organizations
    ADD CONSTRAINT organizations_pkey PRIMARY KEY (org_id);


--
-- Name: pf_payouts pf_payouts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pf_payouts
    ADD CONSTRAINT pf_payouts_pkey PRIMARY KEY (return_id);


--
-- Name: returns returns_org_id_period_end_form_type_source_url_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.returns
    ADD CONSTRAINT returns_org_id_period_end_form_type_source_url_key UNIQUE (org_id, period_end, form_type, source_url);


--
-- Name: returns returns_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.returns
    ADD CONSTRAINT returns_pkey PRIMARY KEY (return_id);


--
-- Name: facts_provenance_entity_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX facts_provenance_entity_idx ON public.facts_provenance USING btree (entity_table, entity_pk);


--
-- Name: grants_funder_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX grants_funder_idx ON public.grants USING btree (funder_org_id);


--
-- Name: grants_purpose_fts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX grants_purpose_fts ON public.grants USING gin (to_tsvector('english'::regconfig, COALESCE(purpose_text, ''::text)));


--
-- Name: grants_recipient_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX grants_recipient_idx ON public.grants USING btree (recipient_org_id);


--
-- Name: grants_recipient_name_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX grants_recipient_name_trgm ON public.grants USING gin (recipient_name_raw public.gin_trgm_ops);


--
-- Name: grants_recipient_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX grants_recipient_state_idx ON public.grants USING btree (recipient_state);


--
-- Name: organizations_name_trgm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX organizations_name_trgm ON public.organizations USING gin (name public.gin_trgm_ops);


--
-- Name: organizations_ntee_code_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX organizations_ntee_code_idx ON public.organizations USING btree (ntee_code);


--
-- Name: organizations_ntee_tags_gin; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX organizations_ntee_tags_gin ON public.organizations USING gin (ntee_tags);


--
-- Name: organizations_state_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX organizations_state_idx ON public.organizations USING btree (state);


--
-- Name: returns_form_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX returns_form_type_idx ON public.returns USING btree (form_type);


--
-- Name: returns_org_year_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX returns_org_year_idx ON public.returns USING btree (org_id, tax_year);


--
-- Name: returns_period_end_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX returns_period_end_idx ON public.returns USING btree (period_end);


--
-- Name: stg_irs_bmf_raw_ein_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX stg_irs_bmf_raw_ein_idx ON public.stg_irs_bmf_raw USING btree (ein);


--
-- Name: stg_irs_bmf_raw_loaded_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX stg_irs_bmf_raw_loaded_idx ON public.stg_irs_bmf_raw USING btree (bmf_loaded_at DESC);


--
-- Name: stg_nccs_pf_raw_ein_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX stg_nccs_pf_raw_ein_idx ON public.stg_nccs_pf_raw USING btree (ein);


--
-- Name: stg_nccs_pf_raw_loaded_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX stg_nccs_pf_raw_loaded_idx ON public.stg_nccs_pf_raw USING btree (nccs_loaded_at DESC);


--
-- Name: grants grants_funder_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.grants
    ADD CONSTRAINT grants_funder_org_id_fkey FOREIGN KEY (funder_org_id) REFERENCES public.organizations(org_id) ON DELETE SET NULL;


--
-- Name: grants grants_recipient_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.grants
    ADD CONSTRAINT grants_recipient_org_id_fkey FOREIGN KEY (recipient_org_id) REFERENCES public.organizations(org_id) ON DELETE SET NULL;


--
-- Name: grants grants_return_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.grants
    ADD CONSTRAINT grants_return_id_fkey FOREIGN KEY (return_id) REFERENCES public.returns(return_id) ON DELETE CASCADE;


--
-- Name: pf_payouts pf_payouts_return_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.pf_payouts
    ADD CONSTRAINT pf_payouts_return_id_fkey FOREIGN KEY (return_id) REFERENCES public.returns(return_id) ON DELETE CASCADE;


--
-- Name: returns returns_org_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.returns
    ADD CONSTRAINT returns_org_id_fkey FOREIGN KEY (org_id) REFERENCES public.organizations(org_id) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict 2dIGo6x7nl11sPODtWm9oaFYA9PkoVHdGYwnbAnW9XfdxHe4Sb3qOZzr6j7HPvc

