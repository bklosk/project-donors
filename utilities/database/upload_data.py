"""
ETL script to upload parsed CSVs into PostgreSQL.

Load order:
1) Load CSVs into staging tables (as-is, text columns).
2) Upsert organizations from parsed_filer_data.csv (by EIN).
3) Upsert returns from IRS index_YYYY.csv (join filer for dates, join orgs by EIN).
4) Insert/Upsert pf_payouts by joining on (org EIN + period_end).
5) Insert grants by joining on (filer_ein + period_end), set funder_org_id = returns.org_id.

Env vars: DATABASE_URL or PGHOST/PGPORT/PGDATABASE/PGUSER/PGPASSWORD
"""

from __future__ import annotations

import io
import os
import re
import sys
import glob
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
from dotenv import load_dotenv, find_dotenv
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import socket

try:
    import psycopg2
    from psycopg2.extras import execute_batch
except ImportError as e:
    raise SystemExit(
        "psycopg2-binary is required. Add it to requirements.txt and install."
    )


ROOT = Path(__file__).resolve().parent
# Best-effort repo root (utilities/database -> repo root is three parents up)
REPO_ROOT = ROOT.parent.parent.parent
DATA_DIR = ROOT / "data"


def _normalize_col(name: str) -> str:
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s)
    return s.strip("_")


def _resolve_ipv4(host: str) -> Optional[str]:
    try:
        for fam, socktype, proto, canonname, sockaddr in socket.getaddrinfo(
            host, None, socket.AF_INET
        ):
            ip, *_ = sockaddr
            return str(ip)
    except Exception:
        return None


def _connect():
    """Establish a Postgres connection with a short connect timeout and
    a quick host:port reachability probe to avoid long hangs.
    """
    # Load .env robustly from repo root or current working directory; do not override existing env
    env_path = find_dotenv(usecwd=True)
    if not env_path:
        env_path = str((REPO_ROOT / ".env"))
    try:
        load_dotenv(env_path, override=False)
    except Exception:
        # Fall back silently if dotenv is unavailable or the file can't be read
        pass

    # Allow override of timeout via env; default to 10s
    try:
        connect_timeout = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))
    except ValueError:
        connect_timeout = 10

    url = os.getenv("DATABASE_URL")
    if url:
        # Ensure connect_timeout is present in the DSN/URL
        try:
            parsed = urlparse(url)
            # Only attempt reachability probe if we have a hostname
            host = parsed.hostname or "localhost"
            port = parsed.port or 5432

            # Fast TCP probe (warn-only) to detect obvious reachability issues
            try:
                with socket.create_connection(
                    (host, port), timeout=min(connect_timeout, 10)
                ):
                    pass
            except OSError as e:
                print(f"WARNING: Reachability probe failed for {host}:{port} — {e}")
                print("Continuing to attempt connection via libpq…")

            # Append connect_timeout to query if not already set
            q = dict(parse_qsl(parsed.query))
            if "connect_timeout" not in q:
                q["connect_timeout"] = str(connect_timeout)
            # Force sslmode=require unless explicitly provided
            # If a CA is provided, prefer verify-full; else require
            sslrootcert = os.getenv("DB_SSLROOTCERT")
            if "sslmode" not in q:
                q["sslmode"] = "verify-full" if sslrootcert else "require"
            # Some networks/clients hang on GSS encryption negotiation; disable it
            if "gssencmode" not in q:
                q["gssencmode"] = "disable"
            new_query = urlencode(q)
            url = urlunparse(parsed._replace(query=new_query))
        except Exception:
            # If anything goes wrong with URL parsing, fall back to passing kwargs
            pass

        # Prefer using keyword args with hostaddr to bypass potential IPv6 issues
        try:
            parsed = urlparse(url)
            kwargs = {
                "dbname": (parsed.path or "/").lstrip("/") or None,
                "user": parsed.username,
                "password": parsed.password,
                "host": parsed.hostname,
                "port": parsed.port or 5432,
                "connect_timeout": connect_timeout,
            }
            # Optional ssl root cert
            sslrootcert = os.getenv("DB_SSLROOTCERT")
            if sslrootcert and os.path.exists(sslrootcert):
                kwargs["sslrootcert"] = sslrootcert
                kwargs["sslmode"] = "verify-full"
            # Keepalives to avoid silent stalls
            kwargs.update(
                {
                    "keepalives": 1,
                    "keepalives_idle": 5,
                    "keepalives_interval": 5,
                    "keepalives_count": 3,
                }
            )
            # Disable GSS encryption negotiation explicitly if supported
            kwargs["gssencmode"] = "disable"
            # hostaddr to avoid IPv6 handshake stalls
            ip = _resolve_ipv4(parsed.hostname) if parsed.hostname else None
            if ip:
                kwargs["hostaddr"] = ip
            if os.getenv("DB_DEBUG"):
                print(
                    f"Connecting: host={kwargs.get('host')} hostaddr={kwargs.get('hostaddr')} port={kwargs.get('port')} db={kwargs.get('dbname')} sslrootcert={'yes' if 'sslrootcert' in kwargs else 'no'}",
                    flush=True,
                )
            try:
                return psycopg2.connect(
                    **{k: v for k, v in kwargs.items() if v is not None}
                )
            except psycopg2.OperationalError as e:
                # Retry without gssencmode if the bundled libpq doesn't support it
                if (
                    "invalid connection option" in str(e).lower()
                    and "gssencmode" in str(e).lower()
                ):
                    kwargs.pop("gssencmode", None)
                    return psycopg2.connect(
                        **{k: v for k, v in kwargs.items() if v is not None}
                    )
                else:
                    raise
        except psycopg2.OperationalError as e:
            print("ERROR: Failed to connect to PostgreSQL using DATABASE_URL.")
            print(str(e))
            print(
                "Troubleshooting: verify credentials, host/port, and whether sslmode=require is needed."
            )
            raise SystemExit(1)

    # Fall back to individual PG* env vars
    host = os.getenv("PGHOST", "localhost")
    port = int(os.getenv("PGPORT", "5432"))
    db = os.getenv("PGDATABASE") or os.getenv("POSTGRES_DB")
    # Support alternative env names seen in GUIs
    user = os.getenv("PGUSER") or os.getenv("POSTGRES_USER") or os.getenv("PG_USERNAME")
    pwd = (
        os.getenv("PGPASSWORD")
        or os.getenv("POSTGRES_PASSWORD")
        or os.getenv("PG_PASSWORD")
    )
    if not (db and user and pwd):
        raise SystemExit("Missing DB env vars. Set DATABASE_URL or PG* variables.")

    # Fast TCP probe (warn-only) to detect obvious reachability issues
    try:
        with socket.create_connection((host, port), timeout=min(connect_timeout, 10)):
            pass
    except OSError as e:
        print(f"WARNING: Reachability probe failed for {host}:{port} — {e}")
        print("Continuing to attempt connection via libpq…")

    try:
        kwargs = {
            "host": host,
            "port": port,
            "dbname": db,
            "user": user,
            "password": pwd,
            "connect_timeout": connect_timeout,
        }
        ip = _resolve_ipv4(host)
        if ip:
            kwargs["hostaddr"] = ip
        sslrootcert = os.getenv("DB_SSLROOTCERT")
        if sslrootcert and os.path.exists(sslrootcert):
            kwargs["sslrootcert"] = sslrootcert
            kwargs["sslmode"] = "verify-full"
        else:
            kwargs["sslmode"] = os.getenv("PGSSLMODE", "require")
        # Keepalives to avoid silent stalls
        kwargs.update(
            {
                "keepalives": 1,
                "keepalives_idle": 5,
                "keepalives_interval": 5,
                "keepalives_count": 3,
                "gssencmode": "disable",
            }
        )
        if os.getenv("DB_DEBUG"):
            print(
                f"Connecting: host={kwargs.get('host')} hostaddr={kwargs.get('hostaddr')} port={kwargs.get('port')} db={kwargs.get('dbname')} sslrootcert={'yes' if 'sslrootcert' in kwargs else 'no'}",
                flush=True,
            )
        try:
            return psycopg2.connect(**kwargs)
        except psycopg2.OperationalError as e:
            # Retry without gssencmode if the bundled libpq doesn't support it
            if (
                "invalid connection option" in str(e).lower()
                and "gssencmode" in str(e).lower()
            ):
                kwargs.pop("gssencmode", None)
                return psycopg2.connect(**kwargs)
            else:
                raise
    except psycopg2.OperationalError as e:
        print(
            "ERROR: Failed to connect to PostgreSQL with provided PG* environment variables."
        )
        print(str(e))
        raise SystemExit(1)


DDL_FUNCTIONS = r"""
-- Flexible date parser to normalize various text formats to DATE
CREATE OR REPLACE FUNCTION parse_flex_date(t TEXT)
RETURNS DATE
LANGUAGE SQL
IMMUTABLE
AS $$
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
$$;
"""


DDL_MAIN = r"""
-- Enable helpful extensions
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- Flexible date parser to normalize various text formats to DATE
CREATE OR REPLACE FUNCTION parse_flex_date(t TEXT)
RETURNS DATE
LANGUAGE SQL
IMMUTABLE
AS $$
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
$$;

-- 1) Organizations (foundations & public charities)
CREATE TABLE IF NOT EXISTS organizations (
  org_id          BIGSERIAL PRIMARY KEY,
  ein             TEXT UNIQUE,                 -- 9-digit string, keep as text
  name            TEXT NOT NULL,
  aka             TEXT,
  ntee_major      TEXT,                        -- fill later (BMF/NCCS)
  ntee_refined    TEXT,
  org_type        TEXT CHECK (org_type IN ('PUBLIC_CHARITY','PRIVATE_FOUNDATION','OTHER')),
  is_foundation   BOOLEAN NOT NULL DEFAULT FALSE,
  address_line1   TEXT,
  city            TEXT,
  state           CHAR(2),
  zip_code        TEXT,
  country         TEXT DEFAULT 'US',
  created_at      TIMESTAMPTZ DEFAULT now(),
  updated_at      TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS organizations_name_trgm ON organizations USING gin (name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS organizations_state_idx ON organizations (state);

-- 2) Returns (1 row per XML filing in the IRS index)
CREATE TABLE IF NOT EXISTS returns (
  return_id       BIGSERIAL PRIMARY KEY,
  org_id          BIGINT REFERENCES organizations(org_id) ON DELETE CASCADE,
  tax_year        INT,
  period_begin    DATE,
  period_end      DATE,
  form_type       TEXT CHECK (form_type IN ('F990','F990PF','F990T','OTHER')),
  index_year      INT,                        -- from index_2024.csv, etc.
  object_id       TEXT,                       -- if present in index csv
  source_url      TEXT NOT NULL,              -- XML URL from index
  downloaded_at   TIMESTAMPTZ,                -- when you fetched it
  UNIQUE (org_id, period_end, form_type, source_url)
);
CREATE INDEX IF NOT EXISTS returns_form_type_idx ON returns (form_type);
CREATE INDEX IF NOT EXISTS returns_org_year_idx ON returns (org_id, tax_year);
CREATE INDEX IF NOT EXISTS returns_period_end_idx ON returns (period_end);
-- Helpful composite index for joins from grants on (org_id, period_end)
CREATE INDEX IF NOT EXISTS returns_org_period_idx ON returns (org_id, period_end);

-- 3) Grants (denormalized rows from 990-PF grant tables)
CREATE TABLE IF NOT EXISTS grants (
  grant_id             BIGSERIAL PRIMARY KEY,
  return_id            BIGINT REFERENCES returns(return_id) ON DELETE CASCADE,
  funder_org_id        BIGINT REFERENCES organizations(org_id) ON DELETE SET NULL, -- same as returns.org_id
  recipient_org_id     BIGINT REFERENCES organizations(org_id) ON DELETE SET NULL, -- null until you match
  recipient_name_raw   TEXT NOT NULL,         -- exact text from filing
  recipient_name_line1 TEXT,
  recipient_name_line2 TEXT,
  recipient_city       TEXT,
  recipient_state      CHAR(2),
  recipient_zip        TEXT,
  recipient_country    TEXT,
  recipient_province   TEXT,
  recipient_postal     TEXT,
  amount_cash          INT,
  amount_noncash       INT,
  amount_total         INT,
  purpose_text         TEXT,
  created_at           TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS grants_funder_idx ON grants (funder_org_id);
CREATE INDEX IF NOT EXISTS grants_recipient_idx ON grants (recipient_org_id);
CREATE INDEX IF NOT EXISTS grants_recipient_state_idx ON grants (recipient_state);
CREATE INDEX IF NOT EXISTS grants_recipient_name_trgm ON grants USING gin (recipient_name_raw gin_trgm_ops);
CREATE INDEX IF NOT EXISTS grants_purpose_fts ON grants USING gin (to_tsvector('english', coalesce(purpose_text,'')));

-- 4) Private-foundation payout metrics (one row per 990-PF return)
CREATE TABLE IF NOT EXISTS pf_payouts (
  return_id              BIGINT PRIMARY KEY REFERENCES returns(return_id) ON DELETE CASCADE,
  distributable_amount   INT,
  qualifying_distributions INT,
  undistributed_income   INT,
  payout_shortfall       INT,
  payout_pressure_index  NUMERIC,     -- 0..1
  fy_end_year            INT,
  fy_end_month           INT,
  computed_at            TIMESTAMPTZ DEFAULT now()
);

-- 5) Provenance for auditability (optional)
CREATE TABLE IF NOT EXISTS facts_provenance (
  prov_id         BIGSERIAL PRIMARY KEY,
  entity_table    TEXT NOT NULL,      -- 'grants','returns','pf_payouts'
  entity_pk       BIGINT NOT NULL,    -- id from that table
  field_name      TEXT NOT NULL,      -- e.g., 'amount_total'
  source_url      TEXT NOT NULL,
  xpath_hint      TEXT,               -- optional
  quote_snippet   TEXT,               -- optional
  ingested_at     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS facts_provenance_entity_idx ON facts_provenance (entity_table, entity_pk);
"""


DDL_STAGING = r"""
-- Staging tables with raw text columns (load CSVs as-is)
CREATE TABLE IF NOT EXISTS stg_filer (
  ein TEXT,
  organizationname TEXT,
  addressline1 TEXT,
  city TEXT,
  state TEXT,
  zipcode TEXT,
  returntype TEXT,
  taxperiodbegin TEXT,
  taxperiodend TEXT,
  taxyear TEXT,
  businessofficer TEXT,
  officertitle TEXT,
  officerphone TEXT,
  organization501ctype TEXT,
  totalrevenue TEXT,
  totalexpenses TEXT,
  netassets TEXT
);

-- Staging indexes to speed up joins
CREATE INDEX IF NOT EXISTS stg_filer_ein_idx ON stg_filer (ein);
CREATE INDEX IF NOT EXISTS stg_filer_period_end_idx ON stg_filer (taxperiodend);

CREATE TABLE IF NOT EXISTS stg_index (
  ein TEXT,
  taxperiodend TEXT,
  index_year TEXT,
  object_id TEXT,
  url TEXT,
  formtype TEXT
);

CREATE INDEX IF NOT EXISTS stg_index_ein_idx ON stg_index (ein);
CREATE INDEX IF NOT EXISTS stg_index_period_end_idx ON stg_index (taxperiodend);

CREATE TABLE IF NOT EXISTS stg_grants (
  filerein TEXT,
  taxperiodend TEXT,
  recipientname TEXT,
  recipientnameline1 TEXT,
  recipientnameline2 TEXT,
  recipientcity TEXT,
  recipientstate TEXT,
  recipientzip TEXT,
  recipientcountry TEXT,
  recipientprovince TEXT,
  recipientpostal TEXT,
  grantamountcash TEXT,
  grantamountnoncash TEXT,
  grantamounttotal TEXT,
  grantpurpose TEXT
);

CREATE INDEX IF NOT EXISTS stg_grants_ein_idx ON stg_grants (filerein);
CREATE INDEX IF NOT EXISTS stg_grants_period_end_idx ON stg_grants (taxperiodend);

CREATE TABLE IF NOT EXISTS stg_pf_payout (
  ein TEXT,
  filername TEXT,
  taxperiodend TEXT,
  fyendyear TEXT,
  fyendmonth TEXT,
  distributableamount TEXT,
  qualifyingdistributions TEXT,
  undistributedincome TEXT,
  payoutshortfall TEXT,
  payoutpressureindex TEXT
);

-- Helpful composite index to speed DISTINCT ON for org upsert
CREATE INDEX IF NOT EXISTS stg_filer_ein_period_idx ON stg_filer (ein, taxperiodend DESC NULLS LAST);
"""


UPSERT_ORGS = r"""
INSERT INTO organizations (ein, name, address_line1, city, state, zip_code, org_type, is_foundation)
SELECT DISTINCT ON (s.ein)
    s.ein,
    s.organizationname,
    s.addressline1,
    s.city,
    s.state,
    s.zipcode,
    CASE
        WHEN s.organization501ctype = '990PF' THEN 'PRIVATE_FOUNDATION'
        WHEN s.organization501ctype IN ('501c3','501c') THEN 'PUBLIC_CHARITY'
        ELSE 'OTHER'
    END,
    COALESCE( (s.organization501ctype = '990PF') OR (s.returntype = '990PF'), FALSE )
FROM stg_filer s
WHERE s.ein IS NOT NULL AND s.ein <> ''
ORDER BY s.ein,
    s.taxperiodend DESC NULLS LAST,
    s.taxyear DESC NULLS LAST
ON CONFLICT (ein) DO UPDATE
SET name = EXCLUDED.name,
  city = COALESCE(EXCLUDED.city, organizations.city),
    state= COALESCE(EXCLUDED.state, organizations.state)
WHERE organizations.name IS DISTINCT FROM EXCLUDED.name
     OR organizations.city IS DISTINCT FROM EXCLUDED.city
     OR organizations.state IS DISTINCT FROM EXCLUDED.state;
"""

INSERT_RETURNS = r"""
INSERT INTO returns (org_id, tax_year, period_begin, period_end, form_type, index_year, object_id, source_url, downloaded_at)
SELECT o.org_id,
    NULLIF(f.taxyear,'')::int,
    parse_flex_date(NULLIF(f.taxperiodbegin,'')),
    parse_flex_date(NULLIF(i.taxperiodend,'')),
    CASE WHEN i.formtype='990PF' THEN 'F990PF'
     WHEN i.formtype='990'   THEN 'F990'
     WHEN i.formtype='990T'  THEN 'F990T'
     ELSE 'OTHER' END,
    NULLIF(i.index_year,'')::int,
    i.object_id,
    i.url,
    now()
FROM stg_index i
JOIN organizations o ON o.ein = i.ein
LEFT JOIN stg_filer f  ON f.ein = i.ein AND parse_flex_date(f.taxperiodend) = parse_flex_date(i.taxperiodend)
WHERE NULLIF(i.url,'') IS NOT NULL
ON CONFLICT (org_id, period_end, form_type, source_url) DO NOTHING;
"""

UPSERT_PF_PAYOUTS = r"""
WITH p_dedup AS (
    SELECT DISTINCT ON (ein, taxperiodend)
                 ein, taxperiodend, filername, fyendyear, fyendmonth,
                 distributableamount, qualifyingdistributions, undistributedincome,
                 payoutshortfall, payoutpressureindex
    FROM stg_pf_payout
    WHERE ein IS NOT NULL AND ein <> ''
    ORDER BY ein, taxperiodend, NULLIF(fyendyear,'')::int DESC NULLS LAST
)
INSERT INTO pf_payouts (return_id, distributable_amount, qualifying_distributions, undistributed_income,
                        payout_shortfall, payout_pressure_index, fy_end_year, fy_end_month)
SELECT r.return_id,
    NULLIF(p.distributableamount,'')::numeric::int,
    NULLIF(p.qualifyingdistributions,'')::numeric::int,
    NULLIF(p.undistributedincome,'')::numeric::int,
    NULLIF(p.payoutshortfall,'')::numeric::int,
    NULLIF(p.payoutpressureindex,'')::numeric,
    NULLIF(p.fyendyear,'')::int,
    NULLIF(p.fyendmonth,'')::int
FROM p_dedup p
JOIN organizations o ON o.ein = p.ein
JOIN returns r ON r.org_id = o.org_id AND r.period_end = parse_flex_date(NULLIF(p.taxperiodend,''))
ON CONFLICT (return_id) DO UPDATE
SET distributable_amount = COALESCE(EXCLUDED.distributable_amount, pf_payouts.distributable_amount),
        qualifying_distributions = COALESCE(EXCLUDED.qualifying_distributions, pf_payouts.qualifying_distributions),
        undistributed_income = COALESCE(EXCLUDED.undistributed_income, pf_payouts.undistributed_income),
        payout_shortfall = COALESCE(EXCLUDED.payout_shortfall, pf_payouts.payout_shortfall),
        payout_pressure_index = COALESCE(EXCLUDED.payout_pressure_index, pf_payouts.payout_pressure_index),
        fy_end_year = COALESCE(EXCLUDED.fy_end_year, pf_payouts.fy_end_year),
        fy_end_month = COALESCE(EXCLUDED.fy_end_month, pf_payouts.fy_end_month);
"""

UPSERT_PF_PAYOUTS_FALLBACK = r"""
-- Fallback: when no exact period_end match, attach to nearest tax_year for same org
WITH p_dedup AS (
    SELECT DISTINCT ON (ein, taxperiodend)
                 ein, taxperiodend, filername, fyendyear, fyendmonth,
                 distributableamount, qualifyingdistributions, undistributedincome,
                 payoutshortfall, payoutpressureindex
    FROM stg_pf_payout
    WHERE ein IS NOT NULL AND ein <> ''
    ORDER BY ein, taxperiodend, NULLIF(fyendyear,'')::int DESC NULLS LAST
)
,
joined AS (
    SELECT p.*, o.org_id, r_exact.return_id AS exact_return_id, r.return_id AS nearest_return_id
    FROM p_dedup p
    JOIN organizations o ON o.ein = p.ein
    LEFT JOIN returns r_exact ON r_exact.org_id = o.org_id AND r_exact.period_end = parse_flex_date(NULLIF(p.taxperiodend,''))
    JOIN LATERAL (
        SELECT r2.return_id, r2.tax_year, r2.period_end
        FROM returns r2
        WHERE r2.org_id = o.org_id
        ORDER BY ABS(r2.tax_year - NULLIF(p.fyendyear,'')::int) ASC, r2.period_end DESC NULLS LAST
        LIMIT 1
    ) r ON TRUE
    WHERE r_exact.return_id IS NULL
)
INSERT INTO pf_payouts (return_id, distributable_amount, qualifying_distributions, undistributed_income,
                                                payout_shortfall, payout_pressure_index, fy_end_year, fy_end_month)
SELECT DISTINCT ON (COALESCE(j.exact_return_id, j.nearest_return_id))
             COALESCE(j.exact_return_id, j.nearest_return_id) AS return_id,
             NULLIF(j.distributableamount,'')::numeric::int,
             NULLIF(j.qualifyingdistributions,'')::numeric::int,
             NULLIF(j.undistributedincome,'')::numeric::int,
             NULLIF(j.payoutshortfall,'')::numeric::int,
             NULLIF(j.payoutpressureindex,'')::numeric,
             NULLIF(j.fyendyear,'')::int,
             NULLIF(j.fyendmonth,'')::int
FROM joined j
ON CONFLICT (return_id) DO UPDATE
SET distributable_amount = COALESCE(EXCLUDED.distributable_amount, pf_payouts.distributable_amount),
    qualifying_distributions = COALESCE(EXCLUDED.qualifying_distributions, pf_payouts.qualifying_distributions),
    undistributed_income = COALESCE(EXCLUDED.undistributed_income, pf_payouts.undistributed_income),
    payout_shortfall = COALESCE(EXCLUDED.payout_shortfall, pf_payouts.payout_shortfall),
    payout_pressure_index = COALESCE(EXCLUDED.payout_pressure_index, pf_payouts.payout_pressure_index),
    fy_end_year = COALESCE(EXCLUDED.fy_end_year, pf_payouts.fy_end_year),
    fy_end_month = COALESCE(EXCLUDED.fy_end_month, pf_payouts.fy_end_month);
"""

INSERT_GRANTS = r"""
-- Build a compact mapping of (org_id, period_end) -> return_id to speed joins
CREATE TEMP TABLE IF NOT EXISTS tmp_return_map (
    org_id BIGINT,
    period_end DATE,
    return_id BIGINT,
    PRIMARY KEY (org_id, period_end)
) ON COMMIT DROP;

TRUNCATE tmp_return_map;

INSERT INTO tmp_return_map (org_id, period_end, return_id)
SELECT r.org_id, r.period_end, r.return_id
FROM (
    SELECT DISTINCT ON (org_id, period_end)
                 org_id, period_end, return_id,
                 (form_type='F990PF') AS is_pf
    FROM returns
    WHERE period_end IS NOT NULL
    ORDER BY org_id, period_end, is_pf DESC, return_id
) r;

-- Insert grants using the precomputed map
INSERT INTO grants (return_id, funder_org_id, recipient_name_raw, recipient_name_line1, recipient_name_line2,
                    recipient_city, recipient_state, recipient_zip, recipient_country, recipient_province,
                    recipient_postal, amount_cash, amount_noncash, amount_total, purpose_text)
SELECT m.return_id, m.org_id,
         COALESCE(NULLIF(g.recipientname,''), NULLIF(g.recipientnameline1,''), NULLIF(g.recipientnameline2,''), 'UNKNOWN'),
         g.recipientnameline1, g.recipientnameline2,
         g.recipientcity, g.recipientstate, g.recipientzip, g.recipientcountry, g.recipientprovince,
         g.recipientpostal,
    NULLIF(g.grantamountcash,'')::numeric::int,
    NULLIF(g.grantamountnoncash,'')::numeric::int,
    NULLIF(g.grantamounttotal,'')::numeric::int,
         g.grantpurpose
FROM stg_grants g
JOIN organizations o ON o.ein = g.filerein
JOIN tmp_return_map m ON m.org_id = o.org_id AND m.period_end = parse_flex_date(NULLIF(g.taxperiodend,''));
"""


INSERT_RETURNS_FROM_GRANTS = r"""
-- Fallback: ensure there is a returns row for each (funder EIN, period_end) present in stg_grants
-- Lightweight version that avoids joins to stg_index/stg_filer to reduce runtime
INSERT INTO returns (org_id, tax_year, period_begin, period_end, form_type, index_year, object_id, source_url, downloaded_at)
SELECT o.org_id,
       NULL,                       -- tax_year unknown here
       NULL,                       -- period_begin unknown
       d.period_end,
       'F990PF',
       NULL,                       -- index_year
       NULL,                       -- object_id
       'synthetic://ein='||d.ein||'&period_end='||d.period_end::text||'&form=F990PF',
       now()
FROM (
    SELECT DISTINCT filerein AS ein, parse_flex_date(NULLIF(taxperiodend,'')) AS period_end
    FROM stg_grants
    WHERE filerein IS NOT NULL AND filerein <> '' AND NULLIF(taxperiodend,'') <> ''
) d
JOIN organizations o ON o.ein = d.ein
WHERE d.period_end IS NOT NULL
ON CONFLICT (org_id, period_end, form_type, source_url) DO NOTHING;
"""


def run_sql(cur, sql: str):
    # If the SQL contains a $$-quoted block (e.g., CREATE FUNCTION), execute as-is
    if "$$" in sql:
        cur.execute(sql)
        return
    # Otherwise, split on semicolons into individual statements
    for stmt in [s.strip() for s in sql.split(";") if s.strip()]:
        cur.execute(stmt + ";")


def truncate_staging(cur):
    cur.execute("TRUNCATE stg_filer;")
    cur.execute("TRUNCATE stg_index;")
    cur.execute("TRUNCATE stg_grants;")
    cur.execute("TRUNCATE stg_pf_payout;")


def df_to_table(cur, df: pd.DataFrame, table: str, columns: List[str]):
    # Keep only expected columns in order; add missing as empty string
    df2 = df.copy()
    for c in columns:
        if c not in df2.columns:
            df2[c] = None
    df2 = df2[columns]
    # Write to CSV buffer and COPY for speed
    buf = io.StringIO()
    df2.to_csv(buf, index=False, header=True)
    buf.seek(0)
    cur.copy_expert(
        f"COPY {table} ({', '.join(columns)}) FROM STDIN WITH CSV HEADER", buf
    )


def load_csvs(conn):
    cur = conn.cursor()
    # Ensure schemas exist
    run_sql(cur, DDL_MAIN)
    run_sql(cur, DDL_STAGING)
    conn.commit()

    truncate_staging(cur)
    conn.commit()

    # 1) stg_filer
    filer_path = DATA_DIR / "parsed_filer_data.csv"
    if filer_path.exists():
        df = pd.read_csv(filer_path, dtype=str, low_memory=False)
        df.columns = [_normalize_col(c) for c in df.columns]
        df_to_table(
            cur,
            df,
            "stg_filer",
            [
                "ein",
                "organizationname",
                "addressline1",
                "city",
                "state",
                "zipcode",
                "returntype",
                "taxperiodbegin",
                "taxperiodend",
                "taxyear",
                "businessofficer",
                "officertitle",
                "officerphone",
                "organization501ctype",
                "totalrevenue",
                "totalexpenses",
                "netassets",
            ],
        )
        conn.commit()
    else:
        print(f"WARNING: Missing {filer_path}")

    # 2) stg_index (multiple files)
    index_files = sorted(glob.glob(str(DATA_DIR / "index_202*.csv")))
    if index_files:
        # Create a temp buffer to accumulate rows across years
        frames = []
        for p in index_files:
            df = pd.read_csv(p, dtype=str, low_memory=False)
            df.columns = [_normalize_col(c) for c in df.columns]
            # Normalize common column names
            rename_map = {}
            if "tax_period_end" in df.columns and "taxperiodend" not in df.columns:
                rename_map["tax_period_end"] = "taxperiodend"
            if "return_type" in df.columns and "formtype" not in df.columns:
                rename_map["return_type"] = "formtype"
            if "return_id" in df.columns and "object_id" not in df.columns:
                rename_map["return_id"] = "object_id"
            if rename_map:
                df = df.rename(columns=rename_map)
            # Add index_year if missing
            if "index_year" not in df.columns:
                m = re.search(r"index_(\d{4})\.csv$", p)
                df["index_year"] = m.group(1) if m else None
            frames.append(df[[c for c in df.columns]])

        if frames:
            df_all = pd.concat(frames, ignore_index=True)
            df_to_table(
                cur,
                df_all,
                "stg_index",
                ["ein", "taxperiodend", "index_year", "object_id", "url", "formtype"],
            )
            conn.commit()
    else:
        print("WARNING: No index_202*.csv files found under data/")

    # 3) stg_pf_payout
    pf_path = DATA_DIR / "parsed_pf_payout.csv"
    if pf_path.exists():
        df = pd.read_csv(pf_path, dtype=str, low_memory=False)
        df.columns = [_normalize_col(c) for c in df.columns]
        df_to_table(
            cur,
            df,
            "stg_pf_payout",
            [
                "ein",
                "filername",
                "taxperiodend",
                "fyendyear",
                "fyendmonth",
                "distributableamount",
                "qualifyingdistributions",
                "undistributedincome",
                "payoutshortfall",
                "payoutpressureindex",
            ],
        )
        conn.commit()
    else:
        print(f"WARNING: Missing {pf_path}")

    # 4) stg_grants
    grants_path = DATA_DIR / "parsed_grants.csv"
    if grants_path.exists():
        df = pd.read_csv(grants_path, dtype=str, low_memory=False)
        df.columns = [_normalize_col(c) for c in df.columns]
        # Normalize EIN and date columns to expected names
        if "filer_ein" in df.columns and "filerein" not in df.columns:
            df = df.rename(columns={"filer_ein": "filerein"})
        if "tax_period_end" in df.columns and "taxperiodend" not in df.columns:
            df = df.rename(columns={"tax_period_end": "taxperiodend"})
        df_to_table(
            cur,
            df,
            "stg_grants",
            [
                "filerein",
                "taxperiodend",
                "recipientname",
                "recipientnameline1",
                "recipientnameline2",
                "recipientcity",
                "recipientstate",
                "recipientzip",
                "recipientcountry",
                "recipientprovince",
                "recipientpostal",
                "grantamountcash",
                "grantamountnoncash",
                "grantamounttotal",
                "grantpurpose",
            ],
        )
        conn.commit()
    else:
        print(f"WARNING: Missing {grants_path}")

    # Done staging
    cur.close()


def transform_and_load(conn):
    cur = conn.cursor()
    # 1) organizations
    run_sql(cur, UPSERT_ORGS)
    conn.commit()
    # 2) returns
    run_sql(cur, INSERT_RETURNS)
    # 2b) returns fallback from grants if index files lack URLs or dates
    # Allow long-running fallback without timing out
    cur.execute("SET LOCAL statement_timeout = '0';")
    run_sql(cur, INSERT_RETURNS_FROM_GRANTS)
    conn.commit()
    # 3) grants (now that returns exist)
    cur.execute("SET LOCAL statement_timeout = '0';")
    run_sql(cur, INSERT_GRANTS)
    conn.commit()
    # 4) pf_payouts (best-effort; don't block grants load)
    try:
        cur.execute("SET LOCAL statement_timeout = '0';")
        run_sql(cur, UPSERT_PF_PAYOUTS)
        run_sql(cur, UPSERT_PF_PAYOUTS_FALLBACK)
        conn.commit()
    except Exception as e:
        print(f"WARNING: pf_payouts upsert skipped due to: {e}")
        conn.rollback()
    cur.close()


def _set_session_settings(conn):
    """Apply optional per-session settings such as statement_timeout."""
    with conn.cursor() as cur:
        # statement timeout (milliseconds), 0 means no timeout
        timeout_ms = os.getenv("DB_STATEMENT_TIMEOUT_MS")
        if timeout_ms:
            try:
                int(timeout_ms)
                cur.execute(f"SET statement_timeout = '{timeout_ms}ms';")
            except Exception:
                # Ignore invalid values
                pass
        else:
            # Default a sane timeout to avoid indefinite hangs during DDL on busy DBs
            cur.execute("SET statement_timeout = '60000ms';")
        # lock timeout to avoid waiting forever on DDL
        lock_timeout_ms = os.getenv("DB_LOCK_TIMEOUT_MS") or "15000"
        try:
            int(lock_timeout_ms)
            cur.execute(f"SET lock_timeout = '{lock_timeout_ms}ms';")
        except Exception:
            pass
        # help identify session in DB dashboards
        cur.execute("SET application_name = 'project-donors-upload';")
    conn.commit()


def main():
    print("Connecting to PostgreSQL…", flush=True)
    with _connect() as conn:
        print("Connected.", flush=True)
        _set_session_settings(conn)

        print("Ensuring main schema (extensions, tables, indexes)…", flush=True)
        with conn.cursor() as cur:
            run_sql(cur, DDL_MAIN)
            # Always ensure functions are present/up to date
            run_sql(cur, DDL_FUNCTIONS)
        conn.commit()

        print("Ensuring staging schema…", flush=True)
        with conn.cursor() as cur:
            run_sql(cur, DDL_STAGING)
        conn.commit()

        print("Loading CSVs into staging…", flush=True)
        load_csvs(conn)

        print("Upserting organizations, returns, payouts, and grants…", flush=True)
        transform_and_load(conn)

    print("Done.", flush=True)


if __name__ == "__main__":
    main()
