import httpx


class InferenceClient:
    def __init__(self, config):
        self.config = config
        self.state = config  # Backward-compatible alias for older callers.

    def _headers(self):
        headers = {
            "Content-Type": "application/json",
        }

        if self.config.backend in {"openrouter", "openai_compatible"}:
            if not self.config.api_key:
                if self.config.backend == "openrouter":
                    raise ValueError("OPENROUTER backend requires an api_key")
            else:
                headers["Authorization"] = f"Bearer {self.config.api_key}"

        return headers

    def _chat_url(self):
        base = self.config.endpoint.rstrip("/")

        if self.config.backend == "openrouter":
            return f"{base}/chat/completions"

        if self.config.backend in {"sglang", "openai_compatible"}:
            if base.endswith("/v1"):
                return f"{base}/chat/completions"
            return f"{base}/v1/chat/completions"

        raise ValueError(f"Unsupported backend: {self.config.backend}")

    def _build_payload(self, messages, **overrides):
        payload = {
            "model": self.config.model_name,
            "messages": messages,
            "temperature": self.config.temperature,
            "max_tokens": self.config.max_tokens,
            "reasoning": {
                "effort": "none",
            },
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

        try:
            return data["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as e:
            raise ValueError(f"Unexpected response format: {data}") from e

    async def health_check(self, timeout=5.0):
        try:
            if self.config.backend in {"sglang", "openai_compatible"}:
                url = f"{self.config.endpoint.rstrip('/')}/health"
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.get(url)
                    response.raise_for_status()
                return True

            if self.config.backend == "openrouter":
                return self.config.api_key is not None

            return False
        except Exception:
            return False
