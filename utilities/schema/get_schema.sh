#!/usr/bin/env bash

# Dump the Postgres schema to docs/schema.sql
# Robust to being run from any directory; loads env from repo root.
set -euo pipefail

# Resolve repo root relative to this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_FILE="$REPO_ROOT/.env"
OUTPUT_DIR="$REPO_ROOT/docs"
OUTPUT_FILE="$OUTPUT_DIR/schema.sql"

mkdir -p "$OUTPUT_DIR"

# Choose pg_dump binary (prefer Homebrew libpq if present)
PG_DUMP_BIN="${PG_DUMP:-}"
if [[ -z "$PG_DUMP_BIN" ]]; then
	for cand in \
		"/opt/homebrew/opt/libpq/bin/pg_dump" \
		"/usr/local/opt/libpq/bin/pg_dump" \
		"$(command -v pg_dump 2>/dev/null || true)"; do
		if [[ -n "$cand" && -x "$cand" ]]; then PG_DUMP_BIN="$cand"; break; fi
	done
fi
if [[ -z "$PG_DUMP_BIN" ]]; then
	echo "Error: pg_dump not found. Install PostgreSQL client tools (brew install libpq) or set PG_DUMP to the binary path." >&2
	exit 1
fi

# Check client version (must be >= 17 for a 17.x server)
CLIENT_VER_STR="$($PG_DUMP_BIN --version 2>&1 || true)"
CLIENT_MAJOR="$(echo "$CLIENT_VER_STR" | sed -n 's/.*pg_dump (PostgreSQL) \([0-9][0-9]*\).*/\1/p')"
if [[ -z "$CLIENT_MAJOR" ]]; then
	echo "Warning: Unable to parse pg_dump version from: $CLIENT_VER_STR" >&2
else
	if [[ "$CLIENT_MAJOR" -lt 17 ]]; then
		echo "Error: pg_dump major version ($CLIENT_MAJOR) is older than server (17)." >&2
		echo "Fix: Use Homebrew client 17.x:" >&2
		echo "  brew install libpq && brew link --overwrite --force libpq" >&2
		echo "Or set PG_DUMP=/opt/homebrew/opt/libpq/bin/pg_dump (or /usr/local/opt/libpq/bin/pg_dump)." >&2
		exit 1
	fi
fi

# Load .env supporting KEY=value, KEY = value, or KEY: value
if [[ -f "$ENV_FILE" ]]; then
	while IFS= read -r line || [[ -n "$line" ]]; do
		# Trim leading/trailing whitespace
		line="${line%%$'\r'}"
		[[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
		if [[ "$line" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*[:=][[:space:]]*(.*)$ ]]; then
			key="${BASH_REMATCH[1]}"; val="${BASH_REMATCH[2]}"
			# Strip inline comments (allow # inside quotes)
			if [[ ! "$val" =~ ^\".*\"$ && ! "$val" =~ ^\'.*\'$ ]]; then
				val="${val%%#*}"
			fi
			val="${val%%[[:space:]]*}"
			# Remove surrounding quotes
			[[ "$val" =~ ^\"(.*)\"$ ]] && val="${BASH_REMATCH[1]}"
			[[ "$val" =~ ^\'(.*)\'$ ]] && val="${BASH_REMATCH[1]}"
			export "$key"="$val"
		fi
	done < "$ENV_FILE"
else
	echo "Warning: $ENV_FILE not found; relying on current environment." >&2
fi

# Map alternative names to libpq-standard envs
if [[ -n "${PG_USERNAME:-}" ]]; then export PGUSER="$PG_USERNAME"; fi
if [[ -n "${PG_PASSWORD:-}" ]]; then export PGPASSWORD="$PG_PASSWORD"; fi
if [[ -n "${PG_HOST:-}" ]]; then export PGHOST="$PG_HOST"; fi
if [[ -n "${PG_PORT:-}" ]]; then export PGPORT="$PG_PORT"; fi
if [[ -n "${PG_DATABASE:-}" ]]; then export PGDATABASE="$PG_DATABASE"; fi

# SSL settings for pg_dump/libpq
if [[ -n "${DB_SSLROOTCERT:-}" && -f "${DB_SSLROOTCERT}" ]]; then
	export PGSSLROOTCERT="$DB_SSLROOTCERT"
fi
if [[ -n "${PG_SSLMODE:-}" ]]; then
	export PGSSLMODE="$PG_SSLMODE"
else
	export PGSSLMODE="require"
fi
export PGGSSENCMODE="disable"
export PGCONNECT_TIMEOUT="10"

# Build connection args: prefer explicit flags if we have host/port/user/db
CONN_ARGS=()
if [[ -n "${PGHOST:-}" || -n "${PGPORT:-}" || -n "${PGUSER:-}" || -n "${PGDATABASE:-}" ]]; then
	[[ -n "${PGHOST:-}" ]] && CONN_ARGS+=( -h "$PGHOST" )
	[[ -n "${PGPORT:-}" ]] && CONN_ARGS+=( -p "$PGPORT" )
	[[ -n "${PGUSER:-}" ]] && CONN_ARGS+=( -U "$PGUSER" )
	[[ -n "${PGDATABASE:-}" ]] && CONN_ARGS+=( -d "$PGDATABASE" )
elif [[ -n "${DATABASE_URL:-}" ]]; then
	# Ensure sslmode and gssencmode in URL if missing
	DBURL="$DATABASE_URL"
	if [[ "$DBURL" != *"sslmode="* ]]; then
		if [[ "$DBURL" == *"?"* ]]; then DBURL+="&sslmode=${PGSSLMODE}"; else DBURL+="?sslmode=${PGSSLMODE}"; fi
	fi
	if [[ "$DBURL" != *"gssencmode="* ]]; then DBURL+="&gssencmode=disable"; fi
	CONN_ARGS+=( --dbname="$DBURL" )
else
	echo "Error: No connection details. Set DATABASE_URL or PG* variables in $ENV_FILE." >&2
	exit 1
fi

echo "Using $("$PG_DUMP_BIN" --version)" >&2
echo "Dumping schema to $OUTPUT_FILE" >&2
"$PG_DUMP_BIN" -s --no-owner --no-privileges -N 'pg_*' -N information_schema "${CONN_ARGS[@]}" > "$OUTPUT_FILE"
echo "Schema written to $OUTPUT_FILE" >&2

