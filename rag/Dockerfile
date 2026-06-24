# RAG 服务镜像:语义 /search(pgvector/Qdrant)+ /ask(OpenAI 兼容)。
# 依赖含 sentence-transformers(torch,本地 embedding/rerank),镜像偏大属正常。
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/models/hf

WORKDIR /app

# 先装依赖(利用层缓存),再拷源码。
COPY pyproject.toml ./
COPY src ./src
RUN pip install --upgrade pip && pip install .

# 模型缓存目录(可挂卷预热,避免每次冷启动下载本地 embedding/rerank 模型)。
RUN mkdir -p /models/hf

# FastAPI HTTP 端口。
EXPOSE 8000

# 入口:uvicorn 起 ASGI 应用。容器内监听 0.0.0.0 才能被其他容器/宿主访问。
CMD ["uvicorn", "vec_stream_rag.app:app", "--host", "0.0.0.0", "--port", "8000"]
