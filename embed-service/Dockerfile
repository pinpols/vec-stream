# 独立 embedding 推理服务镜像。
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HF_HOME=/models/hf

WORKDIR /app

# 先装依赖(利用层缓存),再拷源码。
COPY pyproject.toml ./
COPY src ./src
RUN pip install --upgrade pip && pip install .

# 模型缓存目录(可挂卷预热,避免每次冷启动下载)。
RUN mkdir -p /models/hf

EXPOSE 8200

# 单进程即可:动态批处理调度器是单事件循环内的协程,
# 多副本横向扩容靠多容器,而非单容器多 worker(否则模型副本翻倍)。
CMD ["embed-service"]
