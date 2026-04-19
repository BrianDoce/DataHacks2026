#!/usr/bin/env bash
# Creates the "illuminagrid" secrets scope and registers all required keys.
# Run once per Databricks workspace before any notebook or workflow executes.
#
# Prerequisites:
#   - Databricks CLI installed and authenticated  (databricks configure --token)
#   - All env vars below must be set in the caller's shell — no credentials here.
#
# Usage:
#   export ZENPOWER_API_KEY=...
#   export EIA_API_KEY=...
#   export NREL_API_KEY=...
#   export NREL_EMAIL=...
#   export MAPBOX_TOKEN=...
#   export DATABRICKS_HOST=...
#   export DATABRICKS_TOKEN=...
#   export DATABRICKS_HTTP_PATH=...
#   export ZENPOWER_BASE_URL=...
#   bash databricks/scripts/setup_secrets.sh

set -euo pipefail

SCOPE="illuminagrid"

# ── Verify all required env vars are present ──────────────────────────────────
REQUIRED_VARS=(
    ZENPOWER_API_KEY
    ZENPOWER_BASE_URL
    EIA_API_KEY
    NREL_API_KEY
    NREL_EMAIL
    MAPBOX_TOKEN
    DATABRICKS_HOST
    DATABRICKS_TOKEN
    DATABRICKS_HTTP_PATH
)

for var in "${REQUIRED_VARS[@]}"; do
    if [[ -z "${!var:-}" ]]; then
        echo "ERROR: required env var '$var' is not set." >&2
        exit 1
    fi
done

# ── Create scope (idempotent — ignore error if it already exists) ─────────────
echo "Creating secrets scope: $SCOPE"
databricks secrets create-scope "$SCOPE" 2>/dev/null || \
    echo "  Scope '$SCOPE' already exists — skipping creation."

# ── Register secrets ──────────────────────────────────────────────────────────
register() {
    local key="$1"
    local value="$2"
    echo "  Registering $SCOPE/$key"
    databricks secrets put-secret "$SCOPE" "$key" --string-value "$value"
}

register "zenpower_api_key"     "$ZENPOWER_API_KEY"
register "zenpower_base_url"    "$ZENPOWER_BASE_URL"
register "eia_api_key"          "$EIA_API_KEY"
register "nrel_api_key"         "$NREL_API_KEY"
register "nrel_email"           "$NREL_EMAIL"
register "mapbox_token"         "$MAPBOX_TOKEN"
register "databricks_host"      "$DATABRICKS_HOST"
register "databricks_token"     "$DATABRICKS_TOKEN"
register "databricks_http_path" "$DATABRICKS_HTTP_PATH"

echo ""
echo "Done. Verify with:  databricks secrets list-secrets $SCOPE"
