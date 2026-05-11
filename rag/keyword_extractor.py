import os
import re
from dataclasses import dataclass

import numpy as np


@dataclass
class KeywordExtractionConfig:
    enabled: bool
    top_n: int
    ngram_min: int
    ngram_max: int
    stop_words: str | None
    max_chars: int
    max_length: int

    @classmethod
    def from_env(cls):
        stop_words = os.getenv("RAG_KEYWORD_STOP_WORDS", "english").strip()
        return cls(
            enabled=os.getenv("RAG_KEYWORD_EXTRACTION", "1") == "1",
            top_n=int(os.getenv("RAG_KEYWORD_TOP_N", "8")),
            ngram_min=int(os.getenv("RAG_KEYWORD_NGRAM_MIN", "1")),
            ngram_max=int(os.getenv("RAG_KEYWORD_NGRAM_MAX", "3")),
            stop_words=stop_words or None,
            max_chars=int(os.getenv("RAG_KEYWORD_MAX_CHARS", "6000")),
            max_length=int(os.getenv("RAG_KEYWORD_BERT_MAX_LENGTH", "512")),
        )


class KeyBertKeywordExtractor:
    def __init__(self, config: KeywordExtractionConfig, bert_embedder):
        if not config.enabled:
            raise ValueError("Keyword extraction is disabled")
        try:
            from keybert import KeyBERT
            from keybert.backend import BaseEmbedder
        except ImportError as exc:
            raise RuntimeError(
                "Keyword extraction requires keybert. Install dependencies with: "
                "pip install -r requirements.txt"
            ) from exc

        class ExistingBertBackend(BaseEmbedder):
            def __init__(self, embedder, max_length: int):
                try:
                    super().__init__(embedding_model=embedder)
                except TypeError:
                    super().__init__()
                self.embedder = embedder
                self.max_length = max_length

            def embed(self, documents, verbose: bool = False):
                if isinstance(documents, str):
                    documents = [documents]
                embeddings = [
                    self.embedder.embed_text(
                        document,
                        max_length=self.max_length,
                    ).embedding
                    for document in documents
                ]
                return np.asarray(embeddings, dtype=np.float32)

        self.config = config
        self.bert_embedder = bert_embedder
        self.model = KeyBERT(
            model=ExistingBertBackend(
                bert_embedder,
                max_length=config.max_length,
            )
        )

    @classmethod
    def from_env(cls, bert_embedder):
        config = KeywordExtractionConfig.from_env()
        if not config.enabled:
            return None
        return cls(config, bert_embedder)

    def extract_keywords(self, text: str) -> list[dict]:
        normalized = re.sub(r"\s+", " ", text or "").strip()
        if not normalized:
            return []
        normalized = normalized[: self.config.max_chars]
        keywords = self.model.extract_keywords(
            normalized,
            keyphrase_ngram_range=(self.config.ngram_min, self.config.ngram_max),
            stop_words=self.config.stop_words,
            top_n=self.config.top_n,
        )
        return [
            {
                "keyword": keyword,
                "score": float(score),
            }
            for keyword, score in keywords
        ]
