import os

from common.inference_config import InferenceConfig
from common.inference_client import InferenceClient


def build_inference_client_from_env() -> InferenceClient:
    openrouter_api_key = os.getenv("RAG_OPENROUTER_API_KEY", os.getenv("OPENROUTER_API_KEY", ""))
    chat_api_key = os.getenv(
        "RAG_API_KEY",
        os.getenv("RAG_OPENAI_API_KEY", os.getenv("OPENAI_API_KEY", "")),
    )
    if openrouter_api_key:
        default_backend = "openrouter"
    elif chat_api_key:
        default_backend = "openai_compatible"
    else:
        default_backend = "sglang"
    backend = os.getenv("RAG_BACKEND", os.getenv("BACKEND", default_backend))

    if backend == "openrouter":
        config = InferenceConfig(
            backend="openrouter",
            model_name=os.getenv(
                "RAG_MODEL_NAME",
                os.getenv("MODEL_NAME", "nvidia/nemotron-3-super-120b-a12b:free"),
            ),
            endpoint=os.getenv("RAG_OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1"),
            api_key=openrouter_api_key,
            temperature=float(os.getenv("RAG_TEMPERATURE", "0.0")),
            max_tokens=int(os.getenv("RAG_MAX_TOKENS", "1024")),
        )
    elif backend == "openai_compatible":
        config = InferenceConfig(
            backend="openai_compatible",
            model_name=os.getenv(
                "RAG_MODEL_NAME",
                os.getenv("OPENAI_MODEL", os.getenv("MODEL_NAME", "gpt-5.2")),
            ),
            endpoint=os.getenv(
                "RAG_BASE_URL",
                os.getenv(
                    "OPENAI_BASE_URL",
                    os.getenv(
                        "OPENAI_COMPATIBLE_BASE_URL",
                        "https://api.openai.com/v1" if chat_api_key else "http://127.0.0.1:30000",
                    ),
                ),
            ),
            api_key=os.getenv(
                "RAG_API_KEY",
                os.getenv(
                    "RAG_OPENAI_API_KEY",
                    os.getenv("OPENAI_API_KEY", os.getenv("OPENAI_COMPATIBLE_API_KEY", "")),
                ),
            ),
            temperature=float(os.getenv("RAG_TEMPERATURE", "0.0")),
            max_tokens=int(os.getenv("RAG_MAX_TOKENS", "512")),
        )
    else:
        config = InferenceConfig(
            backend="sglang",
            model_name=os.getenv("RAG_MODEL_NAME", os.getenv("MODEL_NAME", "default-model")),
            endpoint=os.getenv("RAG_SGLANG_BASE_URL", os.getenv("SGLANG_BASE_URL", "http://127.0.0.1:30000")),
            api_key=None,
            temperature=float(os.getenv("RAG_TEMPERATURE", "0.0")),
            max_tokens=int(os.getenv("RAG_MAX_TOKENS", "512")),
        )

    return InferenceClient(config)
