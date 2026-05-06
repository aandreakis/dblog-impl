Output "docs/img/hero.mp4"
Output "docs/img/hero.webm"

# Record at 2× the target display size (supersample) so that lanczos
# downscaling integrates xterm.js's grayscale AA into proper edge
# pixels — crisper text than recording at native target size. FontSize
# and Padding are doubled proportionally to preserve the layout ratios.
# Framerate is 15 because the TUI emits ~7 events/sec; 24 fps captures
# identical frames between events and wastes bytes.
#
# Regenerate hero.gif from hero.webm with the helper script alongside
# this file:
#
#   ./build_hero_gif.sh           # default W=1200, PAD=16 (README target)
#   W=900 PAD=0 ./build_hero_gif.sh   # bare un-framed walkthrough variant
#
# The script wraps the lanczos downscale, the #f6f8fa frame pad,
# palettegen/paletteuse tuning (max_colors=64, dither=none,
# diff_mode=rectangle), and gifsicle -O3. See its header for the full
# rationale.
Set FontSize 36
Set Width 2800
Set Height 1640
Set Padding 20
Set Theme "Blazer"
Set TypingSpeed 0ms
Set PlaybackSpeed 1.0
Set Framerate 15

Hide
Type "./target/release/hydroscope --scenario showcase --slowdown 400"
Enter
Sleep 800ms
Show
Sleep 27s
Hide
Type "q"
Sleep 200ms
