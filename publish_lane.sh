#!/usr/bin/env bash
# publish_lane.sh — Mac-side publisher for the Council Scoreboard Hummingbot lane.
#
# What it does:
#   1. Runs sync_hummingbot.py in --watch mode against the Hummingbot trades
#      DB in ~/hummingbot/data, staging each emitted hummingbot_lane.json in
#      a private temp folder.
#   2. Whenever the staged JSON changes, copies it into the target directory
#      (the local clone of the GitHub Pages repo the scoreboard is served
#      from), as <target>/hummingbot_lane.json.
#   3. ONLY if the target is a git repo AND contains a `.publish-ok` marker
#      file: stages, commits, and pushes the JSON with a timestamped message.
#      Without the marker it copies only — it will never commit or push.
#
# Safety:
#   * No credentials anywhere in this script. `git push` uses whatever
#     credentials Will's own git setup already has for that repo.
#   * Never force-pushes. A failed push prints a warning and the loop keeps
#     running (the next change retries).
#   * Read-only against Hummingbot's data (the sync script opens the DB
#     with mode=ro).
#
# Usage:
#   bash publish_lane.sh <pages-repo-dir>
#
# Optional environment overrides:
#   HB_DATA_DIR   Hummingbot data dir   (default: ~/hummingbot/data)
#   HB_DB         Full path to the trades DB
#                 (default: $HB_DATA_DIR/conf_pure_mm_1.sqlite)

set -u

usage() {
  cat >&2 <<'EOF'
Usage: bash publish_lane.sh <pages-repo-dir>

  <pages-repo-dir>  Local clone of the GitHub Pages repo the Council
                    Scoreboard is deployed from (see PUBLISHING.md).

The script watches the Hummingbot paper bot's trade data, and every time
the lane numbers change it copies hummingbot_lane.json into that folder.
If the folder is a git repo AND contains a .publish-ok file, it also
commits and pushes — otherwise it only copies. No credentials are stored
in this script; git uses your own setup. Ctrl-C stops it.
EOF
}

if [ $# -ne 1 ]; then
  usage
  exit 2
fi

TARGET_DIR="$1"
if [ ! -d "$TARGET_DIR" ]; then
  echo "publish_lane: folder not found: $TARGET_DIR" >&2
  echo "Clone the scoreboard repo first — see PUBLISHING.md." >&2
  exit 1
fi
# Resolve to an absolute path so git -C and cp behave from anywhere.
TARGET_DIR="$(cd "$TARGET_DIR" && pwd)"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SYNC="$SCRIPT_DIR/sync_hummingbot.py"
if [ ! -f "$SYNC" ]; then
  echo "publish_lane: sync_hummingbot.py not found next to this script ($SYNC)" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "publish_lane: python3 not found on PATH." >&2
  exit 1
fi

HB_DATA_DIR="${HB_DATA_DIR:-$HOME/hummingbot/data}"
DB="${HB_DB:-$HB_DATA_DIR/conf_pure_mm_1.sqlite}"

PUBLISH_GIT=0
if [ -d "$TARGET_DIR/.git" ] && [ -f "$TARGET_DIR/.publish-ok" ]; then
  PUBLISH_GIT=1
fi

WORK_DIR="$(mktemp -d)"
STAGE="$WORK_DIR/hummingbot_lane.json"

echo "publish_lane: watching Hummingbot data"
echo "  trades DB : $DB"
echo "  target    : $TARGET_DIR/hummingbot_lane.json"
if [ "$PUBLISH_GIT" -eq 1 ]; then
  echo "  git       : ENABLED (.publish-ok found — changes will be committed and pushed)"
else
  echo "  git       : copy-only (no .publish-ok marker in target — nothing will be committed)"
fi
echo "  stop      : Ctrl-C"

python3 "$SYNC" --source auto --db "$DB" --out "$STAGE" --watch --interval 5 &
SYNC_PID=$!

cleanup() {
  kill "$SYNC_PID" 2>/dev/null
  wait "$SYNC_PID" 2>/dev/null
  rm -rf "$WORK_DIR"
  echo "publish_lane: stopped."
  exit 0
}
trap cleanup INT TERM

last_hash=""
while true; do
  if [ -f "$STAGE" ]; then
    # cksum is POSIX and present on macOS and Linux alike.
    hash="$(cksum "$STAGE" | awk '{print $1"-"$2}')"
    if [ "$hash" != "$last_hash" ]; then
      last_hash="$hash"
      cp "$STAGE" "$TARGET_DIR/hummingbot_lane.json"
      echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] lane updated -> copied to $TARGET_DIR/"
      if [ "$PUBLISH_GIT" -eq 1 ]; then
        git -C "$TARGET_DIR" add hummingbot_lane.json 2>/dev/null
        if git -C "$TARGET_DIR" diff --cached --quiet 2>/dev/null; then
          echo "  git: no change to commit."
        else
          msg="lane sync $(date -u +%Y-%m-%dT%H:%M:%SZ)"
          if git -C "$TARGET_DIR" commit -m "$msg" >/dev/null 2>&1; then
            echo "  git: committed ($msg)"
            if git -C "$TARGET_DIR" push >/dev/null 2>&1; then
              echo "  git: pushed."
            else
              echo "  git: WARNING — push failed (offline? credentials?). Will retry on the next change." >&2
            fi
          else
            echo "  git: WARNING — commit failed (is git user.name/user.email set for this repo?)." >&2
          fi
        fi
      fi
    fi
  fi
  sleep 5
done
