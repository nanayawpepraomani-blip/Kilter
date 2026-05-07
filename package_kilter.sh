#!/usr/bin/env bash
# package_kilter.sh
# Creates a clean zip of the Kilter project safe to send to a tester.
# Excludes: database files, virtual env, secrets, runtime data, pycache.
#
# Usage:
#   bash package_kilter.sh              → kilter_testing.zip
#   bash package_kilter.sh my_name.zip  → my_name.zip

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${1:-kilter_testing.zip}"

# Always produce an absolute path so the output lands next to the project,
# not inside the zip itself if the script is run from a different cwd.
if [[ "$OUT" != /* ]]; then
    OUT="$(pwd)/$OUT"
fi

cd "$SCRIPT_DIR"

echo "Packaging Kilter → $OUT"

# Remove a stale output file so zip doesn't append.
rm -f "$OUT"

zip -r "$OUT" . \
    --exclude "*.pyc" \
    --exclude "./.venv" \
    --exclude "./.venv/*" \
    --exclude "./venv" \
    --exclude "./venv/*" \
    --exclude "./__pycache__" \
    --exclude "./__pycache__/*" \
    --exclude "*/__pycache__" \
    --exclude "*/__pycache__/*" \
    --exclude "./.env" \
    --exclude "./.kilter_secret_key" \
    --exclude "./kilter.db" \
    --exclude "./kilter.db-shm" \
    --exclude "./kilter.db-wal" \
    --exclude "./kilter.db.bak*" \
    --exclude "./first_login.txt" \
    --exclude "./Data/*" \
    --exclude "./exports/*" \
    --exclude "./uploads/*" \
    --exclude "./messages/swift/processed/*" \
    --exclude "./messages/flexcube/processed/*" \
    --exclude "./messages/swift/unloaded/*" \
    --exclude "./messages/flexcube/unloaded/*" \
    --exclude "./*.zip" \
    --exclude "./.DS_Store" \
    --exclude "*/.DS_Store" \
    --exclude "*/Thumbs.db" \
    --exclude "./.idea/*" \
    --exclude "./.vscode/*" \
    --exclude "./*.swp" \
    --exclude "./nohup.out" \
    --exclude "./.git" \
    --exclude "./.git/*" \
    --exclude "./messages/processed/*" \
    --exclude "./messages/unloaded/*"

SIZE=$(du -sh "$OUT" | cut -f1)
echo ""
echo "Done: $OUT  ($SIZE)"
echo ""
echo "Contents preview (top-level):"
unzip -l "$OUT" | awk 'NR>3 && NF==4 {split($4,a,"/"); if(a[2]=="" && a[1]!="") print "  " $4}' | sort | head -30
