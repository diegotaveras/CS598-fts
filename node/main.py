import os
import asyncio
import httpx
from fastapi import FastAPI
import uvicorn

app = FastAPI()

NODE_ID = os.getenv("NODE_ID", "node")
PORT = int(os.getenv("PORT", 8000))

# peers passed like: node2:8000,node3:8000
PEERS = os.getenv("PEERS", "").split(",") if os.getenv("PEERS") else []


@app.get("/ping")
async def ping():
    return {"node": NODE_ID, "status": "alive"}


@app.post("/message")
async def receive_message(data: dict):
    sender = data.get("sender")
    msg = data.get("msg")

    print(f"[{NODE_ID}] received from {sender}: {msg}")

    return {"status": "received"}


async def system_setup():
    """Send hello message to all peers"""
    await asyncio.sleep(2)

    async with httpx.AsyncClient() as client:
        print(PEERS, flush=True)
        for peer in PEERS:
            try:
                url = f"http://{peer}/message"
                payload = {
                    "sender": NODE_ID,
                    "msg": "hello from system setup"
                }

                r = await client.post(url, json=payload)

                print(f"[{NODE_ID}] sent message to {peer} -> {r.status_code}")

            except Exception as e:
                print(f"[{NODE_ID}] failed to reach {peer}: {e}")

@app.on_event("startup")
async def startup_event():
    print(f"Starting now {NODE_ID}", flush=True)
    asyncio.create_task(system_setup())

def main():
    uvicorn.run(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()