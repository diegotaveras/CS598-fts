#!/bin/sh
set -e

cd /app/node
python agent.py &
exec python main.py