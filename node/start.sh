#!/bin/sh
set -e

cd /app/node
python agent_setup.py &
exec python main.py