#!/usr/bin/env bash
# Build the LinkedIn variant of hero.gif from hero.webm.
#
# Differs from build_hero_gif.sh in two ways:
#   1. No GitHub-tuned #f6f8fa padding. LinkedIn renders article images on
#      its own surfaces; the GitHub gray inset would clash with LinkedIn's
#      backgrounds.
#   2. Output goes to build/linkedin/ at the repo root, since this artifact
#      is for an external LinkedIn article rather than the repo README.
#
# Same lanczos + 64-color palette pipeline as build_hero_gif.sh:
#   1. lanczos+full_chroma_int downscale of the 2800x1640 supersampled
#      hero.webm to W wide (default 1200, LinkedIn's preferred image width).
#   2. two-pass palette: palettegen stats_mode=diff with max_colors=64;
#      paletteuse dither=none:diff_mode=rectangle for sharp text edges.
#   3. gifsicle -O3 lossless final pass.
#
# Override width per invocation, e.g.:
#   W=1080 ./build_linkedin_gif.sh
#
# Re-record hero.webm first with `vhs hero.tape` if the TUI changed.

set -euo pipefail

W="${W:-1200}"

cd "$(dirname "$0")"

if [[ ! -f hero.webm ]]; then
  echo "hero.webm not found in $(pwd)." >&2
  echo "Re-record it first with: vhs hero.tape" >&2
  exit 1
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"
OUT_DIR="$REPO_ROOT/build/linkedin"
OUT="$OUT_DIR/hero-linkedin-${W}.gif"
mkdir -p "$OUT_DIR"

PALETTE="$(mktemp -t hero-li-palette.XXXXXX).png"
trap 'rm -f "$PALETTE"' EXIT

ffmpeg -y -loglevel error -i hero.webm -vf \
  "scale=${W}:-2:flags=lanczos+accurate_rnd+full_chroma_int,palettegen=stats_mode=diff:max_colors=64" \
  "$PALETTE"

ffmpeg -y -loglevel error -i hero.webm -i "$PALETTE" -lavfi \
  "[0:v]scale=${W}:-2:flags=lanczos+accurate_rnd+full_chroma_int[x];[x][1:v]paletteuse=dither=none:diff_mode=rectangle" \
  "$OUT"

gifsicle -O3 "$OUT" -o "$OUT"

printf 'Wrote %s (W=%s, no padding)\n' "$OUT" "$W"
ls -la "$OUT"
