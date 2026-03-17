#!/bin/sh
set -e

cd /app/node

if [ "$RESET_LOGS" = "1" ]; then
  ./reset_logs.sh
fi

python agent_setup.py &
python node_main.py &

if [ "$CLIENT_ID" = "1" ]; then
  sleep 3
  exec python client_main.py
else
  wait
fi
