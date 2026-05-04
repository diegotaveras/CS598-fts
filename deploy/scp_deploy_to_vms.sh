#!/bin/sh
set -eu

REMOTE_USER="${REMOTE_USER:-diegotav}"
REMOTE_REPO="${REMOTE_REPO:-~/CS598-fts}"

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

HOSTS="
c220g5-110429.wisc.cloudlab.us
c220g5-120125.wisc.cloudlab.us
c220g5-110430.wisc.cloudlab.us
c220g5-110404.wisc.cloudlab.us
c220g5-110418.wisc.cloudlab.us
c220g5-120127.wisc.cloudlab.us
c220g5-110421.wisc.cloudlab.us
c220g5-110401.wisc.cloudlab.us
"

for HOST in $HOSTS; do
  echo "copying deploy/ to $REMOTE_USER@$HOST:$REMOTE_REPO/"
  scp -r "$REPO_ROOT/deploy" "$REMOTE_USER@$HOST:$REMOTE_REPO/"
done

echo "done"
