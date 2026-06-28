"""Cross-encoder rerank(DESIGN.md §3.6):召回 topK 后用交叉编码器重排取 topN。
bge-reranker 输出相关性分数(越大越相关),与向量余弦分数不在一个量纲。"""

try:
    from sentence_transformers import CrossEncoder
except Exception:  # noqa: BLE001
    # 同 app.py:torch 缺失时降级 None,Reranker 仅在运行期实例化才真用到。
    CrossEncoder = None


class Reranker:
    def __init__(self, model_name: str):
        self.model = CrossEncoder(model_name)

    def rerank(self, query: str, docs: list[str]) -> list[float]:
        if not docs:
            return []
        scores = self.model.predict([(query, d) for d in docs])
        return [float(s) for s in scores]
