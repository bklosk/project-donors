"""
Ingest latest IRS EO BMF and NCCS PF files from data/NCCS into staging tables,
normalize EINs, dedupe per EIN, and upsert NTEE metadata into organizations.

Key behaviors:
- Auto-detect delimiter and encoding; infer headers.
- Stage into stg_irs_bmf_raw and stg_nccs_pf_raw without dropping tables.
- Add/refresh provenance timestamps (bmf_loaded_at, nccs_loaded_at).
- Use IRS BMF NTEE_CD when available. NCCS is used for name enrichment only.
- Backfill/alter organizations columns if missing; maintain indexes.
- Idempotent: dedupe per EIN and update only when values change. Emit summary.

Usage: python utilities/database/ingest_ntee.py
"""

from __future__ import annotations

import io
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import csv
from dotenv import load_dotenv, find_dotenv
import time
import gzip
import importlib.util

# Ensure we can import connection helpers from upload_data.py
THIS_DIR = Path(__file__).resolve().parent
REPO_ROOT = THIS_DIR.parent.parent
# Load .env robustly from repo root or CWD; do not override already-set env
_env_path = find_dotenv(usecwd=True) or str((REPO_ROOT / ".env"))
try:
    load_dotenv(_env_path, override=False)
except Exception:
    pass
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))
try:
    import upload_data  # type: ignore
except Exception:
    # Fallback: add parent folder to path and retry
    sys.path.insert(0, str(THIS_DIR.parent))
    import upload_data  # type: ignore


# Resolve data directory robustly from env/repo root/CWD with fallbacks
def _resolve_data_dir() -> Path:
    env_dir = os.getenv("NCCS_DATA_DIR")
    candidates: List[Path] = []
    if env_dir:
        candidates.append(Path(env_dir))
    candidates.extend(
        [
            REPO_ROOT / "data" / "nccs",
            REPO_ROOT / "data" / "NCCS",
            Path.cwd() / "data" / "nccs",
            Path.cwd() / "data" / "NCCS",
            Path("/data/nccs"),
            Path("/data/NCCS"),
        ]
    )
    for p in candidates:
        try:
            if p.exists() and p.is_dir():
                return p
        except Exception:
            continue
    # Return first candidate (prefer env) even if missing for clear warning
    return candidates[0] if candidates else (REPO_ROOT / "data" / "nccs")


DATA_DIR = _resolve_data_dir()


def _normalize_ein(val: Optional[str]) -> Optional[str]:
    if val is None:
        return None
    s = re.sub(r"\D", "", str(val))
    if not s:
        return None
    if len(s) < 9:
        s = s.zfill(9)
    elif len(s) > 9:
        # Keep last 9 digits (common when leading zeros lost or extra noise)
        s = s[-9:]
    return s


def _has_pyarrow() -> bool:
    """Check if pyarrow is available without importing it (avoids linter errors)."""
    try:
        return importlib.util.find_spec("pyarrow") is not None
    except Exception:
        return False


def _dedupe_columns(cols: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    out: List[str] = []
    for c in cols:
        base = re.sub(r"[^a-z0-9]+", "_", str(c).strip().lower()).strip("_")
        if base == "":
            base = "column"
        if base not in seen:
            seen[base] = 0
            out.append(base)
        else:
            seen[base] += 1
            out.append(f"{base}_{seen[base]}")
    return out


def _auto_read_csv(path: Path, nrows: Optional[int] = None) -> pd.DataFrame:
    """Fast CSV reader without expensive sniffing.

    - Assume comma delimiter and UTF-8 with BOM support.
    - Prefer pyarrow engine if installed; fallback to C then python with on_bad_lines=skip.
    - Explicitly handle gzip files for faster decompression.
    """
    encodings = ["utf-8-sig", "utf-8", "latin-1"]
    use_pyarrow = _has_pyarrow()
    last_err: Optional[Exception] = None
    for enc in encodings:
        try:
            if path.suffix == ".gz":
                with gzip.open(path, mode="rt", encoding=enc, newline="") as f:
                    if use_pyarrow:
                        df = pd.read_csv(
                            f,
                            dtype=str,
                            sep=",",
                            engine="pyarrow",
                            low_memory=False,
                            nrows=nrows,
                        )
                    else:
                        df = pd.read_csv(
                            f,
                            dtype=str,
                            sep=",",
                            engine="c",
                            low_memory=False,
                            nrows=nrows,
                        )
            else:
                if use_pyarrow:
                    df = pd.read_csv(
                        path,
                        dtype=str,
                        encoding=enc,
                        sep=",",
                        engine="pyarrow",
                        low_memory=False,
                        nrows=nrows,
                    )
                else:
                    df = pd.read_csv(
                        path,
                        dtype=str,
                        encoding=enc,
                        sep=",",
                        engine="c",
                        low_memory=False,
                        nrows=nrows,
                    )
            df.columns = _dedupe_columns(list(df.columns))
            return df
        except Exception as e:
            last_err = e
            continue
    # Final tolerant fallback
    try:
        if path.suffix == ".gz":
            with gzip.open(path, mode="rt", encoding="latin-1", newline="") as f:
                df = pd.read_csv(
                    f,
                    dtype=str,
                    sep=",",
                    engine="python",
                    on_bad_lines="skip",
                    low_memory=False,
                    nrows=nrows,
                )
        else:
            df = pd.read_csv(
                path,
                dtype=str,
                encoding="latin-1",
                sep=",",
                engine="python",
                on_bad_lines="skip",
                low_memory=False,
                nrows=nrows,
            )
        df.columns = _dedupe_columns(list(df.columns))
        return df
    except Exception:
        if last_err:
            raise last_err
        raise


def _sanitize_for_json(obj):
    """Recursively replace NaN/NaT with None so jsonb accepts the payload."""
    if isinstance(obj, dict):
        return {k: _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_for_json(v) for v in obj]
    # pandas uses NaN/NaT sentinels for missing
    try:
        # pd.isna handles numpy/pandas sentinels and None
        if pd.isna(obj):
            return None
    except Exception:
        pass
    return obj


def _ensure_staging(cur) -> None:
    cur.execute(
        """
        -- Create if missing
        CREATE TABLE IF NOT EXISTS stg_irs_bmf_raw (
            ein             TEXT,
            bmf_loaded_at   TIMESTAMPTZ DEFAULT now()
        );
        -- Non-destructive schema evolution
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stg_irs_bmf_raw' AND column_name='legal_name') THEN
                ALTER TABLE stg_irs_bmf_raw ADD COLUMN legal_name TEXT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stg_irs_bmf_raw' AND column_name='ntee_cd') THEN
                ALTER TABLE stg_irs_bmf_raw ADD COLUMN ntee_cd TEXT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stg_irs_bmf_raw' AND column_name='data') THEN
                ALTER TABLE stg_irs_bmf_raw ADD COLUMN data JSONB;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stg_irs_bmf_raw' AND column_name='source_file') THEN
                ALTER TABLE stg_irs_bmf_raw ADD COLUMN source_file TEXT;
            END IF;
        END$$;
        CREATE INDEX IF NOT EXISTS stg_irs_bmf_raw_ein_idx ON stg_irs_bmf_raw (ein);
        CREATE INDEX IF NOT EXISTS stg_irs_bmf_raw_loaded_idx ON stg_irs_bmf_raw (bmf_loaded_at DESC);

        CREATE TABLE IF NOT EXISTS stg_nccs_pf_raw (
            ein               TEXT,
            nccs_loaded_at    TIMESTAMPTZ DEFAULT now()
        );
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stg_nccs_pf_raw' AND column_name='legal_name') THEN
                ALTER TABLE stg_nccs_pf_raw ADD COLUMN legal_name TEXT;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stg_nccs_pf_raw' AND column_name='data') THEN
                ALTER TABLE stg_nccs_pf_raw ADD COLUMN data JSONB;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='stg_nccs_pf_raw' AND column_name='source_file') THEN
                ALTER TABLE stg_nccs_pf_raw ADD COLUMN source_file TEXT;
            END IF;
        END$$;
        CREATE INDEX IF NOT EXISTS stg_nccs_pf_raw_ein_idx ON stg_nccs_pf_raw (ein);
        CREATE INDEX IF NOT EXISTS stg_nccs_pf_raw_loaded_idx ON stg_nccs_pf_raw (nccs_loaded_at DESC);
        """
    )


def _ensure_org_columns(cur) -> None:
    # Add missing NTEE-related columns to organizations table as needed
    cur.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='organizations' AND column_name='ntee_code'
            ) THEN ALTER TABLE organizations ADD COLUMN ntee_code TEXT; END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='organizations' AND column_name='ntee_major'
            ) THEN ALTER TABLE organizations ADD COLUMN ntee_major TEXT; END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='organizations' AND column_name='ntee_major_name'
            ) THEN ALTER TABLE organizations ADD COLUMN ntee_major_name TEXT; END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='organizations' AND column_name='ntee_category_name'
            ) THEN ALTER TABLE organizations ADD COLUMN ntee_category_name TEXT; END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='organizations' AND column_name='ntee_source'
            ) THEN ALTER TABLE organizations ADD COLUMN ntee_source TEXT; END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='organizations' AND column_name='ntee_conflict'
            ) THEN ALTER TABLE organizations ADD COLUMN ntee_conflict BOOLEAN DEFAULT FALSE; END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='organizations' AND column_name='ntee_updated_at'
            ) THEN ALTER TABLE organizations ADD COLUMN ntee_updated_at TIMESTAMPTZ DEFAULT now(); END IF;

            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='organizations' AND column_name='ntee_tags'
            ) THEN ALTER TABLE organizations ADD COLUMN ntee_tags TEXT[] DEFAULT '{}'; END IF;
        END$$;

        CREATE INDEX IF NOT EXISTS organizations_ntee_code_idx ON organizations (ntee_code);
        CREATE INDEX IF NOT EXISTS organizations_ntee_tags_gin ON organizations USING gin (ntee_tags);
        """
    )


def _copy_df(
    cur, df: pd.DataFrame, table: str, cols: List[str], chunk_rows: int = 100_000
) -> int:
    if df.empty:
        return 0
    # Ensure columns exist, fill missing with None
    df2 = df.copy()
    for c in cols:
        if c not in df2.columns:
            df2[c] = None
    df2 = df2[cols]
    # Convert dicts to JSON strings for jsonb columns
    if "data" in df2.columns:
        df2["data"] = df2["data"].apply(
            lambda x: (
                json.dumps(_sanitize_for_json(x), ensure_ascii=False)
                if isinstance(x, dict)
                else (x if x is not None else "{}")
            )
        )

    total = 0
    n = len(df2)
    # Chunk to avoid long single COPY commands hitting statement timeouts
    # Allow overriding via env var
    try:
        env_chunk = int(os.getenv("COPY_CHUNK_ROWS", str(chunk_rows)))
        chunk_rows = env_chunk if env_chunk > 0 else chunk_rows
    except Exception:
        pass

    for start in range(0, n, max(1, chunk_rows)):
        end = min(n, start + chunk_rows)
        chunk = df2.iloc[start:end]
        buf = io.StringIO()
        chunk.to_csv(buf, index=False, header=True)
        buf.seek(0)
        # Disable statement timeout locally for the duration of this COPY
        try:
            cur.execute("SET LOCAL statement_timeout = '0';")
        except Exception:
            # If not supported, proceed anyway
            pass
        cur.copy_expert(
            f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH CSV HEADER",
            buf,
        )
        total += len(chunk)
    return total


def _select_col(cols: List[str], candidates: List[str]) -> Optional[str]:
    s = set(cols)
    for c in candidates:
        c2 = re.sub(r"[^a-z0-9]+", "_", c.strip().lower()).strip("_")
        if c2 in s:
            return c2
    return None


def _load_files(
    cur, bmf_paths: Optional[List[Path]] = None, nccs_paths: Optional[List[Path]] = None
) -> Tuple[int, int]:
    """Load BMF and NCCS PF files into staging. Returns (bmf_rows, nccs_rows)."""
    total_bmf = 0
    total_nccs = 0
    if not DATA_DIR.exists():
        print(
            f"WARNING: {DATA_DIR} not found; set NCCS_DATA_DIR or put files under {REPO_ROOT / 'data' / 'nccs'}"
        )
        return (0, 0)
    if bmf_paths is None:
        bmf_paths = sorted(DATA_DIR.glob("bmf*.csv*"))
    if nccs_paths is None:
        nccs_paths = sorted(DATA_DIR.glob("nccs_pf*.csv*"))

    def row_to_record(df: pd.DataFrame, source_file: Path, kind: str) -> pd.DataFrame:
        cols = list(df.columns)
        ein_col = _select_col(
            cols, ["ein", "ein_key", "einkey", "einumber", "einumber", "einnum"]
        )
        name_col = _select_col(
            cols,
            [
                "name",
                "name1",
                "orgname",
                "organizationname",
                "legal_name",
                "nccsname",
                "primary_name",
            ],
        )
        ntee_cd_col = _select_col(
            cols, ["ntee_cd", "nteecd", "ntee", "ntee_cd_txt"]
        )  # IRS BMF
        # NCCS files currently lack NTEE; only use for name enrichment

        if not ein_col:
            # Try to coalesce from any column containing 'ein'
            ein_candidates = [c for c in cols if "ein" in c]
            ein_col = ein_candidates[0] if ein_candidates else None

        if not ein_col:
            # Can't load without EIN anchor
            print(f"WARNING: {source_file.name} has no EIN column; skipped")
            return df.iloc[0:0]

        def norm_ein(x):
            return _normalize_ein(x)

        # Build data records
        recs = pd.DataFrame(
            {
                "ein": df[ein_col].map(norm_ein),
                "legal_name": df[name_col] if name_col else None,
                "ntee_cd": df[ntee_cd_col] if ntee_cd_col else None,
                "data": df.to_dict(orient="records"),
                # if file is outside the repo, fall back to absolute path
                "source_file": (
                    str(source_file.relative_to(REPO_ROOT))
                    if str(source_file).startswith(str(REPO_ROOT))
                    else str(source_file)
                ),
            }
        )
        # Drop rows with no EIN after normalization
        recs = recs[recs["ein"].notna() & (recs["ein"] != "")]
        # Keep only needed columns per table; caller decides
        return recs

    # Load BMF
    for p in bmf_paths:
        try:
            df = _auto_read_csv(p)
            recs = row_to_record(df, p, kind="bmf")
            if not recs.empty:
                cur.execute("SAVEPOINT sp_load_file;")
                try:
                    total_bmf += _copy_df(
                        cur,
                        recs,
                        "stg_irs_bmf_raw",
                        ["ein", "legal_name", "ntee_cd", "data", "source_file"],
                    )
                    cur.execute("RELEASE SAVEPOINT sp_load_file;")
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_load_file;")
                    raise
        except Exception as e:
            print(f"WARNING: Failed to load {p.name}: {e}")

    # Load NCCS PF
    for p in nccs_paths:
        try:
            df = _auto_read_csv(p)
            recs = row_to_record(df, p, kind="nccs")
            if not recs.empty:
                cur.execute("SAVEPOINT sp_load_file;")
                try:
                    total_nccs += _copy_df(
                        cur,
                        recs,
                        "stg_nccs_pf_raw",
                        ["ein", "legal_name", "data", "source_file"],
                    )
                    cur.execute("RELEASE SAVEPOINT sp_load_file;")
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_load_file;")
                    raise
        except Exception as e:
            print(f"WARNING: Failed to load {p.name}: {e}")

    return total_bmf, total_nccs


def _upsert_orgs(cur) -> Tuple[int, int, int]:
    """Upsert organizations with NTEE mapping. Returns (inserted, updated, conflicts)."""
    cur.execute(
        """
WITH bmf_best AS (
    SELECT DISTINCT ON (ein)
        ein,
        NULLIF(btrim(legal_name),'') AS legal_name,
        NULLIF(btrim(ntee_cd),'')    AS ntee_cd,
        bmf_loaded_at,
        (CASE WHEN NULLIF(btrim(ntee_cd),'') IS NOT NULL THEN 1 ELSE 0 END
         + CASE WHEN NULLIF(btrim(legal_name),'') IS NOT NULL THEN 1 ELSE 0 END) AS score
    FROM stg_irs_bmf_raw
    WHERE ein IS NOT NULL AND ein <> ''
    ORDER BY ein, bmf_loaded_at DESC, score DESC
),
nccs_best AS (
    SELECT DISTINCT ON (ein)
        ein,
        NULLIF(btrim(legal_name),'') AS legal_name,
        nccs_loaded_at,
        (CASE WHEN NULLIF(btrim(legal_name),'') IS NOT NULL THEN 1 ELSE 0 END) AS score
    FROM stg_nccs_pf_raw
    WHERE ein IS NOT NULL AND ein <> ''
    ORDER BY ein, nccs_loaded_at DESC, score DESC
),
joined AS (
    SELECT COALESCE(n.ein, b.ein) AS ein,
           COALESCE(n.legal_name, b.legal_name) AS best_name,
           b.ntee_cd AS bmf_ntee,
           NULL::text AS nccs_ntee
    FROM bmf_best b
    FULL OUTER JOIN nccs_best n USING (ein)
),
resolved AS (
    SELECT j.ein,
           j.best_name,
           CASE WHEN j.bmf_ntee  IS NOT NULL THEN j.bmf_ntee ELSE NULL END AS ntee_code,
           CASE WHEN j.bmf_ntee  IS NOT NULL THEN 'irs_bmf' ELSE NULL END AS ntee_source,
           FALSE AS ntee_conflict
    FROM joined j
)
-- Insert new orgs by EIN
, inserted AS (
    INSERT INTO organizations (ein, name, ntee_code, ntee_source, ntee_conflict, ntee_major, ntee_updated_at)
    SELECT r.ein,
           COALESCE(NULLIF(r.best_name,''), 'UNKNOWN'),
           NULLIF(r.ntee_code,''),
           r.ntee_source,
           r.ntee_conflict,
           CASE WHEN NULLIF(r.ntee_code,'') IS NOT NULL THEN left(r.ntee_code,1) ELSE NULL END,
           now()
    FROM resolved r
    WHERE r.ein IS NOT NULL AND r.ein <> ''
      AND NOT EXISTS (SELECT 1 FROM organizations o WHERE o.ein = r.ein)
    RETURNING org_id, ein
),
up_pre AS (
    SELECT o.org_id, o.ein, o.name AS cur_name, r.best_name, r.ntee_code AS new_ntee_code,
           o.ntee_code AS cur_ntee_code, r.ntee_source, r.ntee_conflict
    FROM organizations o
    JOIN resolved r ON r.ein = o.ein
),
updated AS (
    UPDATE organizations o
    SET name = CASE WHEN (o.name IS NULL OR btrim(o.name) = '' OR o.name ILIKE 'unknown') AND r.best_name IS NOT NULL THEN r.best_name ELSE o.name END,
        ntee_code = COALESCE(r.ntee_code, o.ntee_code),
        ntee_source = COALESCE(
            CASE WHEN r.ntee_source IS NOT NULL THEN r.ntee_source ELSE o.ntee_source END,
            o.ntee_source
        ),
        ntee_conflict = COALESCE(r.ntee_conflict, o.ntee_conflict),
        ntee_major = COALESCE(CASE WHEN r.ntee_code IS NOT NULL THEN left(r.ntee_code,1) END, o.ntee_major),
        ntee_updated_at = CASE WHEN (r.ntee_code IS NOT NULL AND r.ntee_code IS DISTINCT FROM o.ntee_code)
                                 OR ( (o.name IS NULL OR btrim(o.name) = '' OR o.name ILIKE 'unknown') AND r.best_name IS NOT NULL )
                               THEN now() ELSE o.ntee_updated_at END
    FROM resolved r
    WHERE o.ein = r.ein
      AND (
           (r.ntee_code IS NOT NULL AND r.ntee_code IS DISTINCT FROM o.ntee_code)
        OR ((o.name IS NULL OR btrim(o.name) = '' OR o.name ILIKE 'unknown') AND r.best_name IS NOT NULL)
        OR (r.ntee_conflict IS DISTINCT FROM o.ntee_conflict)
        OR (r.ntee_source IS NOT NULL AND r.ntee_source IS DISTINCT FROM o.ntee_source)
      )
    RETURNING o.org_id, o.ein
)
SELECT
    (SELECT count(*) FROM inserted) AS inserted,
    (SELECT count(*) FROM updated)  AS updated,
    0 AS conflicts;
        """
    )
    row = cur.fetchone()
    return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)


def _enrich_from_dim(cur) -> None:
    """If dim_ntee exists, fill names and tags based on ntee_code."""
    cur.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'dim_ntee'
            ) THEN
                -- Basic mapping: assume dim_ntee has columns code, major_name, category_name, tags (text[])
                UPDATE organizations o
                SET ntee_major = COALESCE(o.ntee_major, LEFT(o.ntee_code,1)),
                    ntee_major_name = COALESCE(o.ntee_major_name, d.major_name),
                    ntee_category_name = COALESCE(o.ntee_category_name, d.category_name),
                    ntee_tags = CASE
                        WHEN o.ntee_tags IS NULL OR o.ntee_tags = '{}'::text[] THEN d.tags
                        ELSE o.ntee_tags
                    END
                FROM dim_ntee d
                WHERE d.code = o.ntee_code
                  AND o.ntee_code IS NOT NULL;
            END IF;
        END$$;
        """
    )


# --- begin robust connection shim ---
import re
from pathlib import Path as _Path


def _find_project_root(start: _Path) -> _Path:
    cur = start
    for _ in range(8):
        if (cur / ".env").exists() or (cur / ".git").exists():
            return cur
        cur = cur.parent
    return start  # fallback


def _load_env(root: _Path):
    try:
        from dotenv import load_dotenv as _load_dotenv  # pip install python-dotenv

        _load_dotenv(root / ".env")
    except Exception:
        env = {}
        p = root / ".env"
        if p.exists():
            for line in p.read_text().splitlines():
                m = re.match(r"\s*([A-Za-z_]\w*)\s*[:=]\s*(.*\S)\s*$", line)
                if m:
                    env[m.group(1)] = m.group(2)
        os.environ.update(env)


def _build_dburl() -> str:
    dburl = os.getenv("DATABASE_URL")
    if not dburl:
        host = os.getenv("PGHOST", "localhost")
        port = os.getenv("PGPORT", "5432")
        user = os.getenv("PGUSER") or os.getenv("USER", "postgres")
        pwd = os.getenv("PGPASSWORD", "")
        db = os.getenv("PGDATABASE", "postgres")
        auth = f"{user}:{pwd}@" if pwd else f"{user}@"
        dburl = f"postgresql://{auth}{host}:{port}/{db}"
    sep = "&" if "?" in dburl else "?"

    def _has_param(name: str) -> bool:
        key = name.split("=")[0]
        s = str(dburl)
        return f"{key}=" in s

    for kv in ("connect_timeout=5", "sslmode=require", "gssencmode=disable"):
        if not _has_param(kv):
            dburl += f"{sep}{kv}"
            sep = "&"
    return dburl


def _connect_direct():
    root = _find_project_root(_Path(__file__).resolve())
    _load_env(root)
    import psycopg2  # use psycopg2 per requirements.txt

    return psycopg2.connect(_build_dburl())


# --- end robust connection shim ---


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Ingest or debug-scan NTEE CSVs")
    parser.add_argument(
        "--debug-csv",
        action="store_true",
        help="Scan CSVs and print diagnostics without connecting to the database.",
    )
    parser.add_argument(
        "--limit-rows",
        type=int,
        default=200,
        help="When debugging, read at most this many rows per file (default: 200).",
    )
    args = parser.parse_args()

    if args.debug_csv:
        # Pure local diagnostics: never connect to DB
        print(f"CSV debug scan in: {DATA_DIR}")
        if not DATA_DIR.exists():
            print(
                f"ERROR: Data directory {DATA_DIR} not found. Set NCCS_DATA_DIR or place files under {REPO_ROOT / 'data' / 'NCCS'}."
            )
            return
        files = list(DATA_DIR.glob("**/*"))
        bmf_paths = [
            p for p in files if p.is_file() and re.search(r"bmf_", p.name, re.I)
        ]
        nccs_paths = [
            p for p in files if p.is_file() and re.search(r"nccs_pf", p.name, re.I)
        ]

        def _summarize(path: Path):
            try:
                enc, sep = "utf-8", ","
                df = _auto_read_csv(path, nrows=args.limit_rows)
                cols = list(df.columns)
                ein_col = _select_col(
                    cols, ["ein", "ein_key", "einkey", "einumber", "einnum"]
                ) or (next((c for c in cols if "ein" in c), None))
                name_col = _select_col(
                    cols,
                    [
                        "name",
                        "name1",
                        "orgname",
                        "organizationname",
                        "legal_name",
                        "nccsname",
                        "primary_name",
                    ],
                )
                ntee_bmf = (
                    _select_col(cols, ["ntee_cd", "nteecd", "ntee", "ntee_cd_txt"])
                    or "-"
                )
                ntee_nccs = _select_col(cols, ["nteefinal", "nteecc"]) or "-"
                total = len(df)
                valid_ein = 0
                sample_eins: List[str] = []
                if ein_col and ein_col in df.columns:
                    s = df[ein_col].astype(str).map(_normalize_ein)
                    valid_ein = int(s.notna().sum())
                    sample_eins = [x for x in s.dropna().unique()[:3]]
                size_mb = (
                    (path.stat().st_size / (1024 * 1024)) if path.exists() else 0.0
                )
                print(
                    f"- {path.name} | {size_mb:.1f} MB | enc='{enc}' sep='{sep}' | rows~{total} | EIN col='{ein_col}' valid_in_sample={valid_ein}/{total} | NTEE IRS='{ntee_bmf}' NCCS='{ntee_nccs}'"
                )
                if sample_eins:
                    print(f"  sample EINs: {', '.join(sample_eins)}")
                if not ein_col:
                    print("  WARNING: No EIN-like column detected in header.")
            except Exception as e:
                print(f"- {path.name} FAILED to parse: {e}")

        print(f"BMF-like files ({len(bmf_paths)}):")
        for p in sorted(bmf_paths):
            _summarize(p)

        print(f"NCCS PF files ({len(nccs_paths)}):")
        for p in sorted(nccs_paths):
            _summarize(p)

        print("Done.")
        return

    # Normal ingest path (connects to DB)
    # Instrumented logging
    t0 = time.perf_counter()

    def log(msg: str):
        print(f"[+{time.perf_counter()-t0:6.2f}s] {msg}", flush=True)

    log("starting ingest")
    # Narrow, non-recursive discovery to avoid heavy pre-connect IO
    data_dir = DATA_DIR
    bmf_paths = sorted(data_dir.glob("bmf*.csv*"))
    nccs_paths = sorted(data_dir.glob("nccs_pf*.csv*"))
    log(f"discovered files: BMF={len(bmf_paths)} NCCS={len(nccs_paths)} in {data_dir}")

    log("connecting to Postgres…")
    with _connect_direct() as conn:
        log("connected")
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = '30min'; SET lock_timeout = '30s';")
        try:
            upload_data._set_session_settings(conn)
        except Exception:
            pass
        with conn.cursor() as cur:
            _ensure_staging(cur)
            _ensure_org_columns(cur)
            log("loading CSVs into staging…")
            bmf_rows, nccs_rows = _load_files(
                cur, bmf_paths=bmf_paths, nccs_paths=nccs_paths
            )
            inserted, updated, conflicts = _upsert_orgs(cur)
            _enrich_from_dim(cur)
        conn.commit()

    log("ingest complete")
    print(f"- Staged IRS BMF rows: {bmf_rows}")
    print(f"- Staged NCCS PF rows: {nccs_rows}")
    print(f"- Organizations inserted: {inserted}")
    print(f"- Organizations updated: {updated}")
    print(f"- NTEE conflicts detected: {conflicts}")


if __name__ == "__main__":
    main()
