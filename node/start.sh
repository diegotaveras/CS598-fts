#!/bin/sh
set -e

cd /app/node

if [ "$RESET_LOGS" = "1" ]; then
  ./reset_logs.sh
fi

ROLE="${ROLE:-replica}"

if [ "$ROLE" = "client" ]; then
  exec python client_main.py
fi

python agent_setup.py &
exec python node_main.py
