#!/usr/bin/env bash
# Rebuild hero.gif from hero.webm using the documented pipeline.
#
#   1. lanczos+full_chroma_int downscale of the 2800x1640 supersampled
#      source to W wide (W=1200 is the README target; W=900 was the
#      previous default).
#   2. pad PAD px on each side with #f6f8fa so the dark TUI doesn't butt
#      directly against GitHub's white page background. The frame is
#      baked into the GIF because GitHub strips inline style= from
#      rendered markdown.
#   3. two-pass palette: palettegen stats_mode=diff with max_colors=64
#      (the source has ~64 distinct colors, so this is exact); paletteuse
#      dither=none:diff_mode=rectangle gives sharp text edges and compact
#      frame deltas.
#   4. gifsicle -O3 lossless final pass (typically shaves 5-20%).
#
# Defaults: W=1200, PAD=16. Override per invocation, e.g.:
#   W=900 PAD=0 ./build_hero_gif.sh   # bare un-framed walkthrough variant
#
# Re-record hero.webm first with `vhs hero.tape` if the TUI changed.

set -euo pipefail

W="${W:-1200}"
PAD="${PAD:-20}"
PADDED=$((PAD * 2))

cd "$(dirname "$0")"

if [[ ! -f hero.webm ]]; then
  echo "hero.webm not found in $(pwd)." >&2
  echo "Re-record it first with: vhs hero.tape" >&2
  exit 1
fi

PALETTE="$(mktemp -t hero-palette.XXXXXX).png"
trap 'rm -f "$PALETTE"' EXIT

ffmpeg -y -loglevel error -i hero.webm -vf \
  "scale=${W}:-2:flags=lanczos+accurate_rnd+full_chroma_int,pad=iw+${PADDED}:ih+${PADDED}:${PAD}:${PAD}:color=0xf6f8fa,palettegen=stats_mode=diff:max_colors=64" \
  "$PALETTE"

ffmpeg -y -loglevel error -i hero.webm -i "$PALETTE" -lavfi \
  "[0:v]scale=${W}:-2:flags=lanczos+accurate_rnd+full_chroma_int,pad=iw+${PADDED}:ih+${PADDED}:${PAD}:${PAD}:color=0xf6f8fa[x];[x][1:v]paletteuse=dither=none:diff_mode=rectangle" \
  hero.gif

gifsicle -O3 hero.gif -o hero.gif

printf 'Wrote hero.gif (W=%s, PAD=%s)\n' "$W" "$PAD"
ls -la hero.gif
