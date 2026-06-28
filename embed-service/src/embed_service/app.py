"""独立 Embedding 推理服务(bge-small-zh)。

把 worker / rag 进程内的 SentenceTransformer 抽成一个 HTTP 服务,实现
"模型与消费解耦 + 扩容不翻倍模型副本"。

bge 系列前缀约定(与 worker/embedder.py、rag/app.py 一致):
- passage 侧:不加前缀,直接编码;
- query  侧:加检索指令前缀 QUERY_PREFIX。
全部走 normalize_embeddings=True(与现有一致,输出已 L2 归一化,可直接点积)。

动态批处理(micro-batching):
SentenceTransformer.encode 是同步重活,且对 GPU/向量化友好——批越大单条越省。
本服务用一个 asyncio.Queue 把短时间内并发到达的多个 /embed 请求里的文本
合并成一批,统一喂模型一次,再把结果按请求切回去。批触发条件二选一(先到先发):
  - 累计文本数达到 MAX_BATCH;
  - 队首文本等待超过 BATCH_WAIT_MS。
模型编码本身扔到线程池(run_in_executor)避免阻塞事件循环。

限流 / 背压:
- 单请求文本数 > MAX_BATCH → 413(请求实体过大,调用方应自行切片);
- 在途批处理队列积压 > MAX_QUEUE → 429(过载,调用方应退避重试)。
"""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

try:
    from sentence_transformers import SentenceTransformer
except Exception:  # noqa: BLE001
    # torch 缺失时 sentence_transformers 导入会抛 NameError/ImportError;
    # 测试 mock 模型、运行期才真加载,这里降级为 None 让模块可被导入。
    SentenceTransformer = None

from embed_service.tracing import instrument_app, setup_tracing

# 进程启动即接线追踪(OTEL_ENABLED 非 true 时为 no-op,不 import otel,零开销)。
setup_tracing("vec-stream-embed-service")

# ---- 配置(全部走环境变量)---------------------------------------------------
EMBED_MODEL = os.getenv("EMBED_MODEL", "BAAI/bge-small-zh-v1.5")
EMBED_DIM = int(os.getenv("EMBED_DIM", "512"))
# 单批 / 单请求文本数上限;超过单请求拒绝(413)。
MAX_BATCH = int(os.getenv("MAX_BATCH", "64"))
# 动态批处理最大等待窗口(毫秒):队首文本最多等这么久就强制成批发车。
BATCH_WAIT_MS = int(os.getenv("BATCH_WAIT_MS", "10"))
# 在途等待批处理的请求积压上限,超过 → 429 背压。
MAX_QUEUE = int(os.getenv("MAX_QUEUE", "256"))
# 喂给底层 encode 的内部 batch_size(与 worker 现状一致)。
ENCODE_BATCH_SIZE = int(os.getenv("ENCODE_BATCH_SIZE", "32"))
PORT = int(os.getenv("PORT", "8200"))

# bge 系列约定:检索 query 加指令前缀(passage 不加)。
QUERY_PREFIX = os.getenv("EMBED_QUERY_PREFIX", "为这个句子生成表示以用于检索相关文章:")

# 进程级共享状态(模型 + 动态批处理调度器)。
state: dict = {}


def _encode_span(batch_size: int):
    """编码批的可选手动 span(带 batch size)。

    OTEL_ENABLED 非 true 时返回 nullcontext——不 import otel、零开销。
    """
    if os.getenv("OTEL_ENABLED", "false").strip().lower() not in ("true", "1", "yes", "on"):
        from contextlib import nullcontext

        return nullcontext()
    from opentelemetry import trace

    tracer = trace.get_tracer("embed_service")
    return tracer.start_as_current_span(
        "embed.encode_batch", attributes={"embed.batch_size": batch_size}
    )


# ---- 动态批处理调度器 -------------------------------------------------------
class _Job:
    """一个待编码的子批(来自单个 /embed 请求,前缀已展开)。"""

    __slots__ = ("texts", "future", "enqueued_at")

    def __init__(self, texts: list[str], loop: asyncio.AbstractEventLoop):
        self.texts = texts
        self.future: asyncio.Future = loop.create_future()
        self.enqueued_at = time.monotonic()


class BatchScheduler:
    """把并发到达的小请求合并成一批喂模型。

    单 worker 单调度协程:从队列攒 job,凑够 MAX_BATCH 或等满 BATCH_WAIT_MS
    就把这些 job 的文本拼成一个大 list 调一次 encode,再按各 job 长度切回去。
    """

    def __init__(self, model, *, max_batch: int, wait_ms: int, max_queue: int):
        self._model = model
        self._max_batch = max_batch
        self._wait = wait_ms / 1000.0
        self._max_queue = max_queue
        self._queue: asyncio.Queue[_Job] = asyncio.Queue()
        self._task: asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    @property
    def backlog(self) -> int:
        return self._queue.qsize()

    async def submit(self, texts: list[str]) -> list[list[float]]:
        """提交一个子批,等调度器统一编码后返回该子批的向量。"""
        if self._queue.qsize() >= self._max_queue:
            raise HTTPException(
                status_code=429,
                detail=f"embed service overloaded (backlog>{self._max_queue}), retry later",
            )
        loop = asyncio.get_running_loop()
        job = _Job(texts, loop)
        await self._queue.put(job)
        return await job.future

    async def _run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            first = await self._queue.get()
            batch: list[_Job] = [first]
            count = len(first.texts)
            # 收集窗口:在 wait 窗口内继续吸纳后到的 job,直到达 MAX_BATCH。
            deadline = first.enqueued_at + self._wait
            while count < self._max_batch:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                try:
                    nxt = await asyncio.wait_for(self._queue.get(), timeout=remaining)
                except TimeoutError:
                    break
                batch.append(nxt)
                count += len(nxt.texts)

            flat: list[str] = []
            for job in batch:
                flat.extend(job.texts)
            try:
                # encode 是同步 CPU/GPU 重活,扔线程池避免阻塞事件循环。
                vecs = await loop.run_in_executor(None, self._encode, flat)
            except Exception as exc:  # noqa: BLE001 - 把异常回传给每个 job
                for job in batch:
                    if not job.future.done():
                        job.future.set_exception(exc)
                continue
            # 按各 job 的文本数把结果切回去。
            offset = 0
            for job in batch:
                n = len(job.texts)
                if not job.future.done():
                    job.future.set_result(vecs[offset : offset + n])
                offset += n

    def _encode(self, texts: list[str]) -> list[list[float]]:
        if not texts:
            return []
        # 可选手动 span:把"合并后一批喂模型"这一步显式标出来(带 batch size),
        # 挂在当前请求的 trace 下。OTEL_ENABLED 非 true 时 _span 为 nullcontext,零开销。
        with _encode_span(len(texts)):
            vecs = self._model.encode(
                texts, normalize_embeddings=True, batch_size=ENCODE_BATCH_SIZE
            )
        return [v.tolist() for v in vecs]


@asynccontextmanager
async def lifespan(app: FastAPI):
    state["model"] = SentenceTransformer(EMBED_MODEL)
    state["ready"] = True
    scheduler = BatchScheduler(
        state["model"],
        max_batch=MAX_BATCH,
        wait_ms=BATCH_WAIT_MS,
        max_queue=MAX_QUEUE,
    )
    scheduler.start()
    state["scheduler"] = scheduler
    yield
    await scheduler.stop()
    state["ready"] = False


app = FastAPI(title="vec-stream-embed-service", lifespan=lifespan)

# 启用时给 FastAPI 自动埋点:每个 /embed 一个 server span,并自动从上游
# (rag)的 traceparent header 续接 trace,实现 rag → embed-service 串联。
instrument_app(app)


# ---- 请求 / 响应 schema(契约)----------------------------------------------
class EmbedRequest(BaseModel):
    texts: list[str] = Field(..., min_length=1, description="待编码文本,非空")
    kind: str = Field(
        "passage",
        description="passage(不加前缀)| query(加 bge 检索指令前缀)",
    )


class EmbedResponse(BaseModel):
    embeddings: list[list[float]]
    model: str
    dim: int


@app.get("/healthz")
def healthz():
    """模型加载状态 + 当前批处理积压。"""
    ready = bool(state.get("ready"))
    sched = state.get("scheduler")
    return {
        "status": "ok" if ready else "loading",
        "ready": ready,
        "model": EMBED_MODEL,
        "dim": EMBED_DIM,
        "max_batch": MAX_BATCH,
        "backlog": sched.backlog if sched else 0,
    }


@app.post("/embed", response_model=EmbedResponse)
async def embed(req: EmbedRequest):
    if req.kind not in ("passage", "query"):
        raise HTTPException(status_code=422, detail="kind must be 'passage' or 'query'")
    if len(req.texts) > MAX_BATCH:
        raise HTTPException(
            status_code=413,
            detail=f"too many texts: {len(req.texts)} > MAX_BATCH={MAX_BATCH}, split client-side",
        )
    scheduler = state.get("scheduler")
    if scheduler is None:
        raise HTTPException(status_code=503, detail="model not ready")

    # bge 前缀:query 加指令前缀,passage 原样。
    if req.kind == "query":
        texts = [QUERY_PREFIX + t for t in req.texts]
    else:
        texts = list(req.texts)

    embeddings = await scheduler.submit(texts)
    return EmbedResponse(embeddings=embeddings, model=EMBED_MODEL, dim=EMBED_DIM)


def main() -> None:
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
