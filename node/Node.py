import Peers
import asyncio
import Agents

class Node:
    def __init__(self, node_id, host, port, peers):
        self.node_id = str(node_id)
        self.host = host
        self.port = int(port)

        self.hostname = f"node{self.node_id}"
        self.self_addr = f"{self.hostname}:{self.port}"
        self.listen_addr = f"{self.host}:{self.port}"
        self.agent_manager = Agents.AgentManager()
        self.peer_manager = Peers.PeerManager(peers, self.self_addr)

    def connect_agent(self, socket, agent_id="local_agent"):
        self.agent_manager.connect_agent(socket, agent_id)
        
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
                print(f"[{self.NODE_ID}] could not establish contact with {peer}", flush=True)

    async def multicast_prompt(self, prompt, agent_id="local_agent"):
        results = {}

        # send to local agent
        try:
            agent_reply = await self.agent_manager.run_task(
                agent_id=agent_id,
                task_id=f"{self.node_id}-prompt",
                payload=prompt,
            )
            results["local_agent"] = agent_reply
            print(agent_reply, flush=True)
        except Exception as e:
            print(f"[{self.node_id}] failed local agent prompt: {e}", flush=True)
            results["local_agent"] = None

        # multicast to peers
        send_tasks = [
            self.peer_manager.send_message(peer, prompt)
            for peer in self.peer_manager.peers
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



        


        