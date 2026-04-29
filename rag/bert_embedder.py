import argparse
import json
import sys
from dataclasses import dataclass

import numpy as np

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass



DEFAULT_BERT_MODEL = "bert-base-uncased"


@dataclass
class BertEmbedding:
    text: str
    model_name: str
    embedding: list[float]

    def to_json(self) -> str:
        return json.dumps(
            {
                "text": self.text,
                "model_name": self.model_name,
                "dimension": len(self.embedding),
                "embedding": self.embedding,
            },
            indent=2,
            sort_keys=True,
        )


class BertEmbedder:
    def __init__(self, model_name: str = DEFAULT_BERT_MODEL, device: str | None = None):
        try:
            import torch
            from transformers import AutoModel, AutoTokenizer
        except ImportError as exc:
            raise RuntimeError(
                "BERT embeddings require torch and transformers. "
                "Install them before running this file, for example: "
                "pip install torch transformers"
            ) from exc

        self.torch = torch
        self.model_name = model_name
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
        self.model.to(self.device)
        self.model.eval()

    def embed_text(self, text: str, max_length: int = 512) -> BertEmbedding:
        inputs = self.tokenizer(
            text,
            return_tensors="pt",
            truncation=True,
            padding=True,
            max_length=max_length,
        )
        inputs = {key: value.to(self.device) for key, value in inputs.items()}

        with self.torch.no_grad():
            output = self.model(**inputs)

        embedding = self._mean_pool(
            output.last_hidden_state,
            inputs["attention_mask"],
        )
        embedding = self._normalize(embedding)
        return BertEmbedding(
            text=text,
            model_name=self.model_name,
            embedding=embedding[0].detach().cpu().numpy().astype(float).tolist(),
        )

    def _mean_pool(self, token_embeddings, attention_mask):
        mask = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        summed = (token_embeddings * mask).sum(dim=1)
        counts = mask.sum(dim=1).clamp(min=1e-9)
        return summed / counts

    def _normalize(self, embedding):
        norm = embedding.norm(p=2, dim=1, keepdim=True).clamp(min=1e-12)
        return embedding / norm


def cosine_similarity(left: list[float], right: list[float]) -> float:
    left_array = np.asarray(left, dtype=np.float64)
    right_array = np.asarray(right, dtype=np.float64)
    denominator = np.linalg.norm(left_array) * np.linalg.norm(right_array)
    if denominator == 0:
        return 0.0
    return float(np.dot(left_array, right_array) / denominator)


def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Generate a local BERT embedding for text."
    )
    parser.add_argument("text", help="Text to embed.")
    parser.add_argument(
        "--model",
        default=DEFAULT_BERT_MODEL,
        help=f"Hugging Face model name or local model path. Default: {DEFAULT_BERT_MODEL}",
    )
    parser.add_argument(
        "--device",
        default=None,
        help="Torch device to use, such as cpu, cuda, or mps. Defaults to cuda if available, else cpu.",
    )
    parser.add_argument("--max-length", type=int, default=512)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])
    embedder = BertEmbedder(model_name=args.model, device=args.device)
    embedding = embedder.embed_text(args.text, max_length=args.max_length)
    print(embedding.to_json())


if __name__ == "__main__":
    main()
