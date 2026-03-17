#!/bin/sh
set -e

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"

mkdir -p "$LOG_DIR"

for file in "$LOG_DIR"/node*.log; do
  if [ -f "$file" ]; then
    : > "$file"
  fi
done

echo "Reset node log files in $LOG_DIR"
