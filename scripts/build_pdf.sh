#!/usr/bin/env bash
# scripts/build_pdf.sh
# Convert Kilter markdown documents to PDF using pandoc.
#
# Usage:
#   bash scripts/build_pdf.sh           → converts all files, PDFs sit next to the .md
#   bash scripts/build_pdf.sh --out dir → write all PDFs into <dir> instead
#
# Requirements:
#   brew install pandoc
#   brew install --cask basictex        # or: brew install --cask mactex (full, ~4 GB)
#   sudo tlmgr update --self && sudo tlmgr install collection-fontsrecommended

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── output directory (default: same folder as each source file) ──────────────
OUT_DIR=""
if [[ "${1:-}" == "--out" && -n "${2:-}" ]]; then
    OUT_DIR="$2"
    mkdir -p "$OUT_DIR"
fi

# ── preflight ────────────────────────────────────────────────────────────────
if ! command -v pandoc &>/dev/null; then
    echo "ERROR: pandoc not found. Install with: brew install pandoc"
    exit 1
fi

ENGINE=""
for e in pdflatex xelatex lualatex; do
    if command -v "$e" &>/dev/null; then
        ENGINE="$e"
        break
    fi
done
if [[ -z "$ENGINE" ]]; then
    echo "ERROR: no LaTeX engine found (pdflatex / xelatex / lualatex)."
    echo "Install with: brew install --cask basictex"
    exit 1
fi

echo "pandoc $(pandoc --version | head -1)  |  engine: $ENGINE"
echo ""

# ── file list ────────────────────────────────────────────────────────────────
FILES=(
    "$ROOT/demo/ONE_PAGER.md"
    "$ROOT/legal/01_memorandum_and_articles.md"
    "$ROOT/legal/02_msa.md"
    "$ROOT/legal/03_pilot_agreement.md"
    "$ROOT/legal/04_dpa.md"
    "$ROOT/legal/05_privacy_policy.md"
    "$ROOT/legal/06_terms_of_service.md"
)

# ── shared pandoc options ────────────────────────────────────────────────────
PANDOC_OPTS=(
    --pdf-engine="$ENGINE"
    -V geometry:margin=1in
    -V fontsize=11pt
    -V colorlinks=true
    -V linkcolor=blue
)

# ── convert ──────────────────────────────────────────────────────────────────
OK=0
FAIL=0

for SRC in "${FILES[@]}"; do
    if [[ ! -f "$SRC" ]]; then
        echo "  SKIP  $SRC (not found)"
        continue
    fi

    BASENAME="$(basename "$SRC" .md)"

    if [[ -n "$OUT_DIR" ]]; then
        DEST="$OUT_DIR/$BASENAME.pdf"
    else
        DEST="$(dirname "$SRC")/$BASENAME.pdf"
    fi

    printf "  %-50s → %s ... " "$(basename "$SRC")" "$(basename "$DEST")"

    if pandoc "${PANDOC_OPTS[@]}" "$SRC" -o "$DEST" 2>/tmp/pandoc_err; then
        SIZE=$(du -sh "$DEST" | cut -f1)
        echo "OK ($SIZE)"
        (( OK++ )) || true
    else
        echo "FAILED"
        cat /tmp/pandoc_err | sed 's/^/      /'
        (( FAIL++ )) || true
    fi
done

echo ""
echo "Done — $OK converted, $FAIL failed."
