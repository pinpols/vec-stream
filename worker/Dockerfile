# Sync Worker 镜像:消费 Debezium CDC → embed → 写 pgvector/Qdrant。
# 依赖含 sentence-transformers(torch),镜像偏大属正常。
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/models/hf

WORKDIR /app

# 先装依赖(利用层缓存),再拷源码。pyproject 用 hatchling,packages 指向 src/。
COPY pyproject.toml ./
COPY src ./src
RUN pip install --upgrade pip && pip install .

# 模型缓存目录(可挂卷预热,避免每次冷启动下载本地 embedding 模型)。
RUN mkdir -p /models/hf

# Prometheus metrics(slot lag 等),与 METRICS_PORT 默认值一致。
EXPOSE 9100

# 入口:常驻消费进程。
CMD ["python", "-m", "vec_stream_worker.main"]
