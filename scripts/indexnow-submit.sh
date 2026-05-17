#!/usr/bin/env bash
#
# Notify IndexNow-participating search engines (Bing, Yandex, Seznam, Naver,
# Yep, and others) that the DBLog GitHub Pages site has changed. Run it after
# the site redeploys. Google does not use IndexNow; it relies on the sitemap.
#
# Prerequisite: the key file must be live at KEY_LOCATION before submitting -
# IndexNow fetches it to verify ownership. HTTP 403 means it could not verify
# the key there; 202 means accepted with verification still pending.
#
set -euo pipefail

HOST="aandreakis.github.io"
KEY="d1fe985d652efca80c4b41064e87a72a"
KEY_LOCATION="https://${HOST}/dblog-impl/${KEY}.txt"

# Keep this list in sync with sitemap.xml.
URLS=(
  "https://aandreakis.github.io/dblog-impl/"
  "https://aandreakis.github.io/dblog-impl/ops/tap-tui/docs/"
)

url_json=$(printf '"%s",' "${URLS[@]}")
payload="{\"host\":\"${HOST}\",\"key\":\"${KEY}\",\"keyLocation\":\"${KEY_LOCATION}\",\"urlList\":[${url_json%,}]}"

echo "Submitting ${#URLS[@]} URL(s) to IndexNow..."
status=$(curl -sS -o /tmp/indexnow-response -w '%{http_code}' \
  -X POST "https://api.indexnow.org/indexnow" \
  -H "Content-Type: application/json; charset=utf-8" \
  --data "${payload}")

echo "HTTP ${status}"
[ -s /tmp/indexnow-response ] && cat /tmp/indexnow-response && echo

case "${status}" in
  200) echo "OK - IndexNow accepted the submission." ;;
  202) echo "OK - accepted; key validation still pending (normal on the first run)." ;;
  400) echo "FAILED - malformed request." >&2; exit 1 ;;
  403) echo "FAILED - key not verified; ensure ${KEY_LOCATION} is live and contains exactly the key." >&2; exit 1 ;;
  422) echo "FAILED - a submitted URL is outside the key's host/path, or key schema mismatch." >&2; exit 1 ;;
  429) echo "FAILED - rate limited (too many requests)." >&2; exit 1 ;;
  *)   echo "FAILED - unexpected response ${status}." >&2; exit 1 ;;
esac
