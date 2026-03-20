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
SELF_ADDR = os.getenv("SELF_ADDR", f"{NODE_ID}:{PORT}")
AGENT_SOCKET_PATH = os.getenv("AGENT_SOCKET_PATH", "/tmp/agent.sock")
CLIENT_ADDR = os.getenv("CLIENT_ADDR", "node1:9000")
LOG_PATH = os.getenv("LOG_PATH")

class NetworkServicer(network_pb2_grpc.NetworkServiceServicer):
    def __init__(self, node):
        self.node = node
    async def Ping(self, request, context):
        print(f"[{NODE_ID}] got ping from {request.sender}", flush=True)
        self.node.log_event("ping_received", sender=request.sender)
        return network_pb2.PingReply(node_id=str(NODE_ID), status="alive")

    async def HandleProtocolMessage(self, request, context):

        if request.HasField("client_request"):
            cr = request.client_request
            print(
                f"[{NODE_ID}] received client_request from {request.sender}: "
                f"request_id={cr.request_id} client_id={cr.client_id} prompt={cr.prompt}",
                flush=True,
            )
            self.node.log_event(
                "client_request_rpc_received",
                sender=request.sender,
                request_id=cr.request_id,
                client_id=cr.client_id,
                prompt=cr.prompt,
                timestamp=cr.timestamp,
            )
            asyncio.create_task(self.node.handle_client_request(cr))
        elif request.HasField("ordered_request"):
            orq = request.ordered_request
            print(
                f"[{NODE_ID}] received ordered_request from {request.sender}: "
                f"request_id={orq.client_request.request_id} "
                f"seqno={orq.seqno} digest={orq.request_digest[:12]} "
                f"leader_result_digest={orq.leader_result_digest[:12]}",
                flush=True,
            )
            self.node.log_event(
                "ordered_request_rpc_received",
                sender=request.sender,
                request_id=orq.client_request.request_id,
                client_id=orq.client_request.client_id,
                seqno=orq.seqno,
                view=orq.view,
                request_digest=orq.request_digest,
                history_digest=orq.history_digest,
                leader_result_digest=orq.leader_result_digest,
            )
            asyncio.create_task(self.node.handle_ordered_request(orq, request.sender))
        elif request.HasField("fill_hole_request"):
            fhr = request.fill_hole_request
            print(
                f"[{NODE_ID}] received fill_hole_request from {request.sender}: "
                f"replica_id={fhr.replica_id} start={fhr.start_seqno} end={fhr.end_seqno}",
                flush=True,
            )
            self.node.log_event(
                "fill_hole_request_rpc_received",
                sender=request.sender,
                replica_id=fhr.replica_id,
                view=fhr.view,
                start_seqno=fhr.start_seqno,
                end_seqno=fhr.end_seqno,
            )
            asyncio.create_task(self.node.handle_fill_hole_request(fhr, request.sender))
        elif request.HasField("fill_hole_response"):
            fhs = request.fill_hole_response
            print(
                f"[{NODE_ID}] received fill_hole_response from {request.sender}: "
                f"responder_id={fhs.responder_id} count={len(fhs.ordered_requests)}",
                flush=True,
            )
            self.node.log_event(
                "fill_hole_response_rpc_received",
                sender=request.sender,
                responder_id=fhs.responder_id,
                view=fhs.view,
                ordered_request_count=len(fhs.ordered_requests),
            )
            asyncio.create_task(self.node.handle_fill_hole_response(fhs, request.sender))

        
        return network_pb2.MessageReply(status="received")


node = Node.Node(NODE_ID, HOST, PORT, PEERS, CLIENT_ADDR, LOG_PATH)


async def node_loop(node):
    # run networking handshake
    await node.handshake_loop()
    
    # testing a prompt multicast to all nodes (including itself)
    if node.node_id == "node1":
        pass
        # asyncio.create_task(node.multicast_prompt(prompt="explain RAFT consensus protocol"))
    

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
    node.log_event("server_starting", listen_addr=listen_addr)

    node.peer_manager.connect_all()

    await server.start()
    print(f"[{NODE_ID}] gRPC server started", flush=True)
    node.log_event("server_started", listen_addr=listen_addr)

    # connect to agent socket
    node.connect_agent(f"unix://{AGENT_SOCKET_PATH}", "local_agent")
    agent_ready = await node.agent_health_check("local_agent")
    if not agent_ready:
        print(f"[{NODE_ID}] proceeding without ready agent", flush=True)
        node.log_event("agent_not_ready")

    asyncio.create_task(node_loop(node))

    await server.wait_for_termination()


def main():
    asyncio.run(serve())


if __name__ == "__main__":
    main()
