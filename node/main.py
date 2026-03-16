import os
import asyncio
import grpc
import network_pb2
import network_pb2_grpc
from grpc_reflection.v1alpha import reflection
import Node

NODE_ID = os.getenv("NODE_ID", "node")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))
PEERS = os.getenv("PEERS", "").split(",") if os.getenv("PEERS") else []
SELF_ADDR = os.getenv("SELF_ADDR", f"node{NODE_ID}:{PORT}")
AGENT_SOCKET_PATH = os.getenv("AGENT_SOCKET_PATH", "/tmp/agent.sock")

class NetworkServicer(network_pb2_grpc.NetworkServiceServicer):
    def __init__(self, node):
        self.node = node
    async def Ping(self, request, context):
        print(f"[{NODE_ID}] got ping from {request.sender}", flush=True)
        return network_pb2.PingReply(node_id=str(NODE_ID), status="alive")

    async def SendMessage(self, request, context):
        print(f"[{NODE_ID}] received from {request.sender}: {request.msg}", flush=True)

        asyncio.create_task(
            node.process_prompt(request.msg, request.sender)
        )
        
        return network_pb2.MessageReply(status="received")


node = Node.Node(NODE_ID, HOST, PORT, PEERS)


async def node_loop(node):
    # run networking handshake
    await node.handshake_loop()
    
    # testing a prompt multicast to all nodes (including itself)
    if node.node_id == "1":
        asyncio.create_task(node.multicast_prompt(prompt="explain RAFT consensus protocol"))
    

async def serve():
    server = grpc.aio.server()
    network_pb2_grpc.add_NetworkServiceServicer_to_server(NetworkServicer(node), server)

    SERVICE_NAMES = (
        network_pb2.DESCRIPTOR.services_by_name["NetworkService"].full_name,
        reflection.SERVICE_NAME,
    )

    reflection.enable_server_reflection(SERVICE_NAMES, server)
    listen_addr = f"{HOST}:{PORT}"
    server.add_insecure_port(listen_addr)

    print(f"[{NODE_ID}] starting gRPC server on {listen_addr}", flush=True)

    node.peer_manager.connect_all()

    await server.start()
    print(f"[{NODE_ID}] gRPC server started", flush=True)

    # connect to agent socket
    node.connect_agent(f"unix://{AGENT_SOCKET_PATH}", "local_agent")
    agent_ready = await node.agent_health_check("local_agent")
    if not agent_ready:
        print(f"[{NODE_ID}] proceeding without ready agent", flush=True)

    asyncio.create_task(node_loop(node))
        
    await server.wait_for_termination()


def main():
    asyncio.run(serve())


if __name__ == "__main__":
    main()