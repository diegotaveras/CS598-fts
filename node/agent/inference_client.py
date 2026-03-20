import httpx


class InferenceClient:
    def __init__(self, agent_state):
        self.state = agent_state

    def _headers(self):
        headers = {
            "Content-Type": "application/json",
        }

        if self.state.backend == "openrouter":
            if not self.state.api_key:
                raise ValueError("OPENROUTER backend requires an api_key")
            headers["Authorization"] = f"Bearer {self.state.api_key}"

        return headers

    def _chat_url(self):
        base = self.state.endpoint.rstrip("/")

        if self.state.backend == "openrouter":
            return f"{base}/chat/completions"

        if self.state.backend == "sglang":
            return f"{base}/v1/chat/completions"

        raise ValueError(f"Unsupported backend: {self.state.backend}")

    def _build_payload(self, messages, **overrides):
        payload = {
            "model": self.state.model_name,
            "messages": messages,
            "temperature": self.state.temperature,
            "max_tokens": self.state.max_tokens,
            "reasoning": {
                "effort": "none"
            }
        }

        payload.update(overrides)
        return payload

    async def run_inference(self, messages, timeout=120.0, **overrides):
        url = self._chat_url()
        headers = self._headers()
        payload = self._build_payload(messages, **overrides)

        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()

        return data

    async def get_text(self, messages, timeout=120.0, **overrides):
        data = await self.run_inference(messages, timeout=timeout, **overrides)
        # print("RAW INFERENCE RESPONSE:", data, flush=True)

        try:
            content = data["choices"][0]["message"]["content"]
            # print("EXTRACTED CONTENT:", repr(content), flush=True)
            return content
        except (KeyError, IndexError, TypeError) as e:
            raise ValueError(f"Unexpected response format: {data}") from e

    async def health_check(self, timeout=5.0):
        try:
            if self.state.backend == "sglang":
                url = f"{self.state.endpoint.rstrip('/')}/health"
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                return True

            if self.state.backend == "openrouter":
                return self.state.api_key is not None

            return False
        except Exception:
            return False