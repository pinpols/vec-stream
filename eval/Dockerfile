# 评估 harness 镜像:检索质量(recall@k/MRR)、生成质量、PG↔源一致性对账。
# 一次性 CLI,非常驻服务。RAGAS 是重依赖(optional),默认不装,走轻量回退实现。
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 先装依赖(利用层缓存),再拷源码。
COPY pyproject.toml ./
COPY src ./src
RUN pip install --upgrade pip && pip install .

# golden 数据集(检索/对账基准),供运行期读取。
COPY golden ./golden

# 入口:CLI。可在 docker run / compose 覆盖参数(如子命令 retrieval/generation/reconcile)。
# project.scripts 暴露 vec-stream-eval,等价 python -m vec_stream_eval。
ENTRYPOINT ["vec-stream-eval"]
CMD ["--help"]
