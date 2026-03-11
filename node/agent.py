import os
import asyncio
import grpc

import agent_pb2
import agent_pb2_grpc
from grpc_reflection.v1alpha import reflection

NODE_ID = os.getenv("NODE_ID", "node")
AGENT_SOCKET_PATH = os.getenv("AGENT_SOCKET_PATH", "/tmp/agent.sock")

class AgentServicer(agent_pb2_grpc.AgentServiceServicer):
    def __init__(self, node_id: str):
        self.node_id = str(node_id)
        self.ready = True
        self.agent_state = {}

    async def HealthCheck(self, request, context):
        return agent_pb2.HealthReply(
            node_id=self.node_id,
            status="ready" if self.ready else "not_ready",
        )

    async def RunTask(self, request, context):
        task_id = request.task_id
        payload = request.payload

        print(f"[agent {self.node_id}] received task {task_id}: {payload}", flush=True)

        result = f"processed: {payload}"

        return agent_pb2.TaskReply(
            task_id=task_id,
            status="ok",
            result=result,
        )
    

def agent_setup():
    print(f"[{NODE_ID}]: Setting up agent")
    # here we can actually load model and tools
    pass


async def serve():

    agent_state = agent_setup()

    # remove stale socket file if it exists
    if os.path.exists(AGENT_SOCKET_PATH):
        os.remove(AGENT_SOCKET_PATH)

    server = grpc.aio.server()
    servicer = AgentServicer(NODE_ID)

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