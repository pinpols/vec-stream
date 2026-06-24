# embed-service — 独立 Embedding 推理服务

把 worker / rag 进程内的 `SentenceTransformer("BAAI/bge-small-zh-v1.5")`(512 维)抽成
一个独立 HTTP 服务,实现 **模型与消费解耦 + 扩容不翻倍模型副本**。worker/rag 改成
HTTP 调用本服务即可,无需各自常驻一份模型。

ENTERPRISE.md M2 领域二 item 5。

## 用途

- 单一模型推理端点,供 worker(passage 侧批量编码)与 rag(query 侧编码)共用。
- 动态批处理:把短时间并发的小请求合并成一批喂模型,提升吞吐。
- 横向扩容靠多副本(多容器),每容器单进程单模型,消费端扩容不再翻倍模型内存。

## 起法

本地(用 rag 同款 Python 3.12 环境即可):

```bash
cd embed-service
pip install .                  # 或 uv pip install -e .
embed-service                  # 等价 uvicorn,监听 0.0.0.0:$PORT(默认 8200)
# 或:python -m embed_service.app
```

Docker:

```bash
cd embed-service
docker build -t vecstream-embed-service .
docker run -p 8200:8200 \
  -v $HOME/.cache/huggingface:/models/hf \   # 预热模型缓存,避免冷启动下载
  vecstream-embed-service
```

## 配置(环境变量)

| 变量 | 默认 | 说明 |
|------|------|------|
| `EMBED_MODEL` | `BAAI/bge-small-zh-v1.5` | SentenceTransformer 模型名 |
| `EMBED_DIM` | `512` | 输出维度(响应里回显;须与模型一致) |
| `MAX_BATCH` | `64` | 单请求 / 单批文本数上限,单请求超限 → 413 |
| `BATCH_WAIT_MS` | `10` | 动态批处理最大等待窗口(毫秒) |
| `MAX_QUEUE` | `256` | 在途批处理积压上限,超限 → 429 |
| `ENCODE_BATCH_SIZE` | `32` | 喂给底层 `encode` 的内部 batch_size(与 worker 现状一致) |
| `EMBED_QUERY_PREFIX` | `为这个句子生成表示以用于检索相关文章:` | bge query 指令前缀 |
| `PORT` | `8200` | 监听端口 |

## API 契约

### `POST /embed`

请求体:

```json
{
  "texts": ["第一段文本", "第二段文本"],
  "kind": "passage"
}
```

- `texts`: `list[str]`,非空(空 → 422)。
- `kind`: `"passage"`(默认,不加前缀) | `"query"`(自动加 bge 检索指令前缀)。
  其它值 → 422。

响应体:

```json
{
  "embeddings": [[0.01, -0.02, "..."], [0.03, 0.04, "..."]],
  "model": "BAAI/bge-small-zh-v1.5",
  "dim": 512
}
```

- `embeddings`: 顺序与 `texts` 一一对应;每条 `EMBED_DIM` 维;`normalize_embeddings=True`
  已 L2 归一化(可直接点积当余弦相似度,与现有写入 pgvector / Qdrant 的向量同分布)。
- 错误码:`413` 单请求超 `MAX_BATCH`;`429` 服务过载(积压超 `MAX_QUEUE`),调用方退避重试;
  `422` 非法 kind / 空 texts;`503` 模型未就绪。

### `GET /healthz`

```json
{
  "status": "ok",
  "ready": true,
  "model": "BAAI/bge-small-zh-v1.5",
  "dim": 512,
  "max_batch": 64,
  "backlog": 0
}
```

`status` 为 `ok`(就绪)或 `loading`(模型加载中)。

## bge 前缀约定(与现有一致)

- **passage**:不加前缀,直接编码(worker 写入侧)。
- **query**:加指令前缀 `为这个句子生成表示以用于检索相关文章:`(rag 检索侧)。

调用方传 `kind`,前缀由本服务统一处理,worker/rag 侧不要再重复加前缀。

## 动态批处理

`encode` 是同步重活、对批量友好。服务内单调度协程从 asyncio 队列攒 job,
**累计文本数达 `MAX_BATCH` 或 队首等满 `BATCH_WAIT_MS`**(先到先发)就把多个请求的
文本拼成一批调一次 `encode`,再按各请求长度切回去。编码本身扔线程池,不阻塞事件循环。

## 与 worker / rag 的接入契约(供消费端改造参考)

消费端用 env `EMBED_SERVICE_URL` 切换:
- **非空** → 走 HTTP 调本服务:
  - worker 写入侧:`POST /embed` `{"texts": <chunks>, "kind": "passage"}`,取 `embeddings`,
    替换 `Embedder.embed_passages` 的进程内 encode;
  - rag 检索侧:`POST /embed` `{"texts": [query], "kind": "query"}`,取 `embeddings[0]`,
    替换 `state["model"].encode(QUERY_PREFIX + query, ...)`——**注意 query 前缀交给本服务,
    rag 侧不要再拼 `QUERY_PREFIX`**。
- **空** → 保持现有进程内模型(回退路径不变)。

向量分布一致:同模型、同 `normalize_embeddings=True`,HTTP 路径与进程内路径产出可互换,
无需重建已写入的索引。调用方应对单请求文本数自行控制在 `MAX_BATCH` 内(超大批先切片),
并对 `429` 做退避重试。

## 测试

```bash
cd embed-service
python -m pytest tests -q     # 全程 mock SentenceTransformer,不加载真模型
```
