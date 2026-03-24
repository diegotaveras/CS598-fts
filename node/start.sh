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

# If using sglang backend, launch the local inference server and wait for it
if [ "$BACKEND" = "sglang" ]; then
  SGLANG_PORT="${SGLANG_PORT:-30000}"
  MODEL="${MODEL_NAME:-qwen/qwen2.5-0.5b-instruct}"

  echo "[start.sh] launching sglang server: model=$MODEL port=$SGLANG_PORT"
  python3 -m sglang.launch_server \
    --model-path "$MODEL" \
    --host 127.0.0.1 \
    --port "$SGLANG_PORT" \
    --log-level warning &

  # Poll health endpoint until ready (timeout ~120s)
  TRIES=0
  MAX_TRIES=60
  until [ "$TRIES" -ge "$MAX_TRIES" ]; do
    if python3 -c "import httpx; r = httpx.get('http://127.0.0.1:${SGLANG_PORT}/health'); r.raise_for_status()" 2>/dev/null; then
      echo "[start.sh] sglang server is ready"
      break
    fi
    TRIES=$((TRIES + 1))
    sleep 2
  done

  if [ "$TRIES" -ge "$MAX_TRIES" ]; then
    echo "[start.sh] ERROR: sglang server did not become ready in time" >&2
    exit 1
  fi
fi

python agent_setup.py &
exec python node_main.py
