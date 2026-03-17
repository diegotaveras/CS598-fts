import Peers
import asyncio
import Agents
from protocol.proto_state import ProtocolState
import network_pb2

class Node:
    def __init__(self, node_id, host, port, peers, client_addr):
        self.node_id = str(node_id)
        self.host = host
        self.port = int(port)
        self.client_addr = client_addr

        self.hostname = f"node{self.node_id}"
        self.self_addr = f"{self.hostname}:{self.port}"
        self.listen_addr = f"{self.host}:{self.port}"
        self.agent_manager = Agents.AgentManager()
        self.peer_manager = Peers.PeerManager(peers, self.self_addr)

        self.proto = ProtocolState(
        node_id=node_id,
        peers=peers,
        role="replica",
        f=1,
        current_view=0,
        primary_id="2",
    )

    def connect_agent(self, socket, agent_id="local_agent"):
        self.agent_manager.connect_agent(socket, agent_id)
        
    async def process_prompt(self, prompt, sender):
        try:
            agent_reply = await self.agent_manager.run_task(
                agent_id="local_agent",
                task_id=f"{sender}-to-{self.node_id}",
                payload=prompt,
            )

            agent_result = agent_reply.result if agent_reply is not None else "agent failed"
            print(
                f"[{self.node_id}] local agent processed message from {sender}: {agent_result}",
                flush=True,
            )
        except Exception as e:
            print(
                f"[{self.node_id}] failed to forward message to local agent: {e}",
                flush=True,
            )

    async def handshake_loop(self):
        await asyncio.sleep(2)

        for peer in self.peer_manager.peers:
            ok = False
            for attempt in range(5):
                ok = await self.peer_manager.ping_peer(peer, timeout=2.0)
                if ok:
                    break

                await asyncio.sleep(1.0)

            if not ok:
                print(f"[{self.node_id}] could not establish contact with {peer}", flush=True)

    async def multicast_prompt(self, request):
        results = {}
        prompt = request.prompt

        # send to local agent
        asyncio.create_task(self.process_prompt(prompt, self.node_id))

        # multicast to peers
        msg = network_pb2.ProtocolMessage(
            sender=self.self_addr,
            ordered_request=network_pb2.OrderedRequest(
                request_id=request.request_id,
                client_id=request.client_id,
                view=self.proto.current_view,
                seqno=0,
                prompt=request.prompt,
                leader_id="2",
            ),
        )
      
        send_tasks = [
            self.peer_manager.send_message(peer, msg)
            for peer in self.peer_manager.peers if peer != self.client_addr
        ]

        send_results = await asyncio.gather(*send_tasks, return_exceptions=True)

        for peer, result in zip(self.peer_manager.peers, send_results):
            if isinstance(result, Exception):
                print(f"[{self.node_id}] failed to multicast to {peer}: {result}", flush=True)
                results[peer] = False
            else:
                results[peer] = result

        return results


    async def agent_health_check(self, agent_id="local_agent", retries=10, delay=1.0):
        for attempt in range(retries):
            ok = await self.agent_manager.health_check(agent_id)
            if ok:
                print(f"[{self.node_id}] agent {agent_id} is ready", flush=True)
                return True

            print(
                f"[{self.node_id}] waiting for agent {agent_id} "
                f"(attempt {attempt + 1}/{retries})",
                flush=True,
            )
            await asyncio.sleep(delay)

        print(f"[{self.node_id}] agent {agent_id} failed health check", flush=True)
        return False
    
    async def handle_client_request(self, request):
        asyncio.create_task(self.multicast_prompt(request))

    async def handle_ordered_request(self, request, sender):
        asyncio.create_task(self.process_prompt(request.prompt, sender))


        


        