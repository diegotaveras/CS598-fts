## Installation

A local Docker container based on `sglang` v0.5.2 compatible with CUDA 12.6 ([here](https://hub.docker.com/layers/lmsysorg/sglang/v0.5.2-cu126/images/sha256-fce5585fa8da175224ead70727a4660f719e34adf0638aebf6dd27930f03e1b6)) is used as the backbone of the program.

Run 
```bash
    docker build --platform linux/amd64 -t <image_name> .
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
