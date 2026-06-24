"""vecstream-eval:RAG 评估模块。

三个子系统:
  retrieval   —— 检索质量(recall@k / MRR),换 chunk 策略/embedding 模型前后跑分对比
  generation  —— 生成质量(RAGAS 或轻量 faithfulness / 引用覆盖率)
  reconcile   —— 一致性对账(doc_vectors vs 源表行数,漂移监控)
"""

__all__ = ["__version__"]
__version__ = "0.1.0"
