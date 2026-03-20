import os
import asyncio
import grpc
from agent import agent_state, inference_client

import agent_pb2
import agent_pb2_grpc
from grpc_reflection.v1alpha import reflection

NODE_ID = os.getenv("NODE_ID", "node")
AGENT_SOCKET_PATH = os.getenv("AGENT_SOCKET_PATH", "/tmp/agent.sock")


class AgentServicer(agent_pb2_grpc.AgentServiceServicer):
    def __init__(self, node_id: str, client):
        self.node_id = str(node_id)
        self.ready = True
        self.client = client

    async def HealthCheck(self, request, context):
        return agent_pb2.HealthReply(
            node_id=self.node_id,
            status="ready" if self.ready else "not_ready",
        )

    async def RunTask(self, request, context):
        task_id = request.task_id
        payload = request.payload

        print(f"[agent {self.node_id}] received task {task_id}: {payload}", flush=True)

        messages = [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": payload},
        ]

        result = await self.client.get_text(messages)

        return agent_pb2.TaskReply(
            task_id=task_id,
            status="ok",
            result=result,
        )


async def agent_setup():
    print(f"[{NODE_ID}]: Setting up agent", flush=True)

    backend = os.getenv("BACKEND", "openrouter")

    if backend == "openrouter":
        state = agent_state.AgentState(
            backend=backend,
            model_name="nvidia/nemotron-3-super-120b-a12b:free",
            endpoint="https://openrouter.ai/api/v1",
            api_key=os.getenv("OPENROUTER_API_KEY", ""),
            max_tokens=1024
        )
    else:
        state = agent_state.AgentState(
            backend="sglang",
            model_name=os.getenv("MODEL_NAME", "default-model"),
            endpoint=os.getenv("SGLANG_BASE_URL", "http://127.0.0.1:30000"),
            api_key=None,
        )

    client = inference_client.InferenceClient(state)
    return client


async def serve():
    client = await agent_setup()

    if os.path.exists(AGENT_SOCKET_PATH):
        os.remove(AGENT_SOCKET_PATH)

    server = grpc.aio.server()
    servicer = AgentServicer(NODE_ID, client)

    agent_pb2_grpc.add_AgentServiceServicer_to_server(servicer, server)

    SERVICE_NAMES = (
        agent_pb2.DESCRIPTOR.services_by_name["AgentService"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    listen_addr = f"unix://{AGENT_SOCKET_PATH}"
    server.add_insecure_port(listen_addr)

    print(f"[agent {NODE_ID}] starting local gRPC server on {listen_addr}", flush=True)

    await server.start()
    print(f"[agent {NODE_ID}] local agent server started", flush=True)

    await server.wait_for_termination()


def main():
    asyncio.run(serve())


if __name__ == "__main__":
    main()