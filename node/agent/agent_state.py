class AgentState:
    def __init__(
        self,
        backend: str,
        model_name: str,
        endpoint: str,
        api_key: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 512,
        metadata: dict | None = None,
    ):
        self.backend = backend  # "openrouter" | "sglang"
        self.model_name = model_name
        self.endpoint = endpoint # base URL
        self.api_key = api_key

        self.temperature = temperature
        self.max_tokens = max_tokens

        self.metadata = metadata or {}