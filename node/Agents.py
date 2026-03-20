import agent_pb2
import agent_pb2_grpc
import grpc

class AgentManager:
    def __init__(self):
        self.channels = {}
        self.stubs = {}

    def connect_agent(self, socket, agent_id):
        channel = grpc.aio.insecure_channel(socket)
        stub = agent_pb2_grpc.AgentServiceStub(channel)
        self.channels[agent_id] = channel
        self.stubs[agent_id] = stub

    async def close_all(self):
        for channel in self.channels.values():
            await channel.close()

    async def health_check(self, agent_id, timeout=2.0):
        try:
            reply = await self.stubs[agent_id].HealthCheck(
                agent_pb2.HealthRequest(),
                timeout=timeout,
            )
            print(f"[agent-manager] {agent_id} health -> {reply.status}", flush=True)
            return reply.status == "ready"
        except Exception as e:
            print(f"[agent-manager] health check failed for {agent_id}: {e}", flush=True)
            return False

    async def run_task(self, agent_id, task_id, payload, timeout=120.0):
        try:
            reply = await self.stubs[agent_id].RunTask(
                agent_pb2.TaskRequest(task_id=task_id, payload=payload),
                timeout=timeout,
            )
            return reply
        except Exception as e:
            print(f"[agent-manager] run_task failed for {agent_id}: {e}", flush=True)
            return None