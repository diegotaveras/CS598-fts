#!/bin/sh
set -e

ROLE="${ROLE:-replica}"

if [ "$ROLE" = "rag_worker" ]; then
  cd /app
  exec python -m rag.worker_main
fi

cd /app/node

if [ "$RESET_LOGS" = "1" ]; then
  ./reset_logs.sh
fi

if [ "$ROLE" = "client" ]; then
  exec python client_main.py
fi

python agent_setup.py &
exec python node_main.py
