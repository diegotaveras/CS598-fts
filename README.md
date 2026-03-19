## Installation

It is recommended to use uv for faster installation:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install uv
uv pip install -r requirements.txt
```

## Debug Logs

Reset node log files manually:
```bash
./node/reset_logs.sh
```

Or clear them automatically on startup:
```bash
RESET_LOGS=1 docker compose up --build
```

Unit tests:
```bash
python -m unittest node/test.py
```
