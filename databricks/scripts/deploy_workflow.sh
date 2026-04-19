#!/usr/bin/env bash
# Deploys the IlluminaGrid pipeline workflow to a Databricks workspace.
#
# Creates the job on first run; updates it in-place on subsequent runs by
# looking up the existing job ID from the workspace before deciding whether
# to call `jobs create` or `jobs update`.
#
# Prerequisites:
#   - Databricks CLI v0.18+ installed  (pip install databricks-cli  OR  brew install databricks)
#   - Workspace authenticated via one of:
#       databricks configure --token          (interactive)
#       env vars DATABRICKS_HOST + DATABRICKS_TOKEN
#   - setup_secrets.sh already run so the `illuminagrid` secrets scope exists
#     and contains `team_email`.
#
# Usage:
#   bash databricks/scripts/deploy_workflow.sh
#
# Optional overrides:
#   WORKFLOW_JSON   path to the job definition  (default: databricks/workflows/illuminagrid_pipeline.json)
#   JOB_NAME        job name to look up          (default: illuminagrid_pipeline)

set -euo pipefail

WORKFLOW_JSON="${WORKFLOW_JSON:-databricks/workflows/illuminagrid_pipeline.json}"
JOB_NAME="${JOB_NAME:-illuminagrid_pipeline}"

# ── Resolve repo root so the script works from any working directory ──────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
WORKFLOW_PATH="${REPO_ROOT}/${WORKFLOW_JSON}"

# ── Validate prerequisites ────────────────────────────────────────────────────
if ! command -v databricks &>/dev/null; then
    echo "ERROR: Databricks CLI not found. Install with:" >&2
    echo "       pip install databricks-cli   # legacy CLI" >&2
    echo "       brew install databricks       # new CLI (recommended)" >&2
    exit 1
fi

if [[ ! -f "${WORKFLOW_PATH}" ]]; then
    echo "ERROR: Workflow definition not found at: ${WORKFLOW_PATH}" >&2
    exit 1
fi

echo "Deploying IlluminaGrid pipeline workflow"
echo "  Definition : ${WORKFLOW_PATH}"
echo "  Job name   : ${JOB_NAME}"
echo ""

# ── Look up existing job ID by name ──────────────────────────────────────────
# `databricks jobs list --output JSON` returns a JSON object with a "jobs" array.
# We extract the ID of the first job whose settings.name matches JOB_NAME.
EXISTING_JOB_ID=$(
    databricks jobs list --output JSON 2>/dev/null \
    | python3 -c "
import json, sys
data = json.load(sys.stdin)
jobs = data.get('jobs') or []
match = next((j['job_id'] for j in jobs if j.get('settings', {}).get('name') == '${JOB_NAME}'), None)
print(match if match is not None else '')
"
)

if [[ -z "${EXISTING_JOB_ID}" ]]; then
    # ── Create new job ────────────────────────────────────────────────────────
    echo "No existing job found — creating '${JOB_NAME}'..."
    CREATE_OUTPUT=$(databricks jobs create --json @"${WORKFLOW_PATH}")
    JOB_ID=$(echo "${CREATE_OUTPUT}" | python3 -c "import json,sys; print(json.load(sys.stdin)['job_id'])")
    echo ""
    echo "Job created successfully."
    echo "  Job ID : ${JOB_ID}"
else
    # ── Reset (full replace) existing job ─────────────────────────────────────
    # `jobs reset` replaces ALL settings from the JSON, equivalent to a PUT.
    echo "Found existing job ID ${EXISTING_JOB_ID} — resetting settings..."

    # `jobs reset` requires the job_id field inside the payload.
    RESET_PAYLOAD=$(python3 -c "
import json, sys
with open('${WORKFLOW_PATH}') as f:
    payload = json.load(f)
payload['job_id'] = ${EXISTING_JOB_ID}
print(json.dumps(payload))
")

    databricks jobs reset --json "${RESET_PAYLOAD}"
    JOB_ID="${EXISTING_JOB_ID}"
    echo ""
    echo "Job updated successfully."
    echo "  Job ID : ${JOB_ID}"
fi

# ── Print quick-access links ──────────────────────────────────────────────────
DATABRICKS_HOST="${DATABRICKS_HOST:-$(databricks configure --help 2>/dev/null | grep -o 'https://[^ ]*' | head -1 || echo '<workspace-url>')}"

echo ""
echo "── Next steps ────────────────────────────────────────────────────────────"
echo "  View job   : ${DATABRICKS_HOST}/#job/${JOB_ID}"
echo "  Run now    : databricks jobs run-now --job-id ${JOB_ID}"
echo "  List runs  : databricks runs list --job-id ${JOB_ID} --output JSON"
echo "──────────────────────────────────────────────────────────────────────────"
