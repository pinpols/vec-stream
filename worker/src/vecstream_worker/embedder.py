"""bge-small-zh 本地 embedding。passage 侧直接编码,query 侧加检索指令前缀
(bge 系列约定,前缀只加在查询上,见 rag 服务)。"""
from sentence_transformers import SentenceTransformer


class Embedder:
    def __init__(self, model_name: str):
        self.model = SentenceTransformer(model_name)

    def embed_passages(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        vecs = self.model.encode(texts, normalize_embeddings=True, batch_size=32)
        return [v.tolist() for v in vecs]
