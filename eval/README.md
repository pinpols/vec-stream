# vec-stream-eval — RAG 评估模块

项目收尾高地(ENTERPRISE.md M2 领域五 item 6)。给 RAG 链路提供**客观、可重复**的跑分,
支撑「换 chunk 策略 / 换 embedding 模型 / 换 rerank 前后对比」与「上线后漂移监控」。

三个子命令:

| 子命令 | 评什么 | 依赖 |
|--------|--------|------|
| `retrieval`  | 检索质量:recall@k、MRR(打真 `/search`) | 跑起来的 rag 服务 + `RAG_API_KEY` |
| `generation` | 生成质量:faithfulness / 引用覆盖率(打真 `/ask`) | rag + `RAG_API_KEY` + `OPENAI_API_KEY` |
| `reconcile`  | 一致性对账:`doc_vectors` vs 源表行数,漂移监控 | 只读 PG DSN |

## 安装

```bash
cd eval
pip install -e .            # 基础(httpx / psycopg / pydantic)
pip install -e '.[ragas]'   # 可选:生成质量用 RAGAS 重依赖(不装走轻量自实现)
```

## 环境变量

- `RAG_API_KEY`：调 `/search`、`/ask` 必带的 `X-API-Key`(租户由 key 推导,不可自选)。
- `RAG_URL`：rag 服务地址,默认 `http://localhost:8000`。
- `OPENAI_API_KEY`：`/ask` 真跑需要;缺失时 `generation` 跳过并提示(不算失败)。
- `RAG_PG_DSN` / `PG_DSN`：`reconcile` 连 PG 的 DSN(只读即可)。

## 用法

```bash
# 检索质量:recall@5 + MRR,golden set 默认读 eval/golden/queries.jsonl
python -m vec_stream_eval retrieval -k 5 --json-out retrieval.json
python -m vec_stream_eval retrieval -k 10 --rerank      # 开 rerank 对比

# 生成质量:引用覆盖率 / 带引用句子比
python -m vec_stream_eval generation --json-out gen.json

# 一致性对账:漏处理(missing)/ 残留(orphan)
python -m vec_stream_eval reconcile --dsn "$RAG_PG_DSN" --json-out drift.json
```

## 指标含义

### 检索(retrieval.py)
- **recall@k** = 前 k 个召回结果中命中的期望文档数 / 期望文档总数。
  同一文档的多个 chunk 去重,不重复计数。期望文档排在 k 之后即视为未召回。
- **MRR**(Mean Reciprocal Rank)= 每条 query 取「第一个命中期望文档的名次」倒数 `1/rank`,
  整个 golden set 取这些倒数的平均(某 query 完全没命中记 0)。

> **换模型决策依据**:换 embedding 模型 / chunk 策略 / 开关 rerank,各跑一次本命令,
> 比较 `mean recall@k` 与 `MRR`。指标涨才换,客观挡住「凭感觉调参」。`--json-out`
> 落盘历史,便于 A/B 留痕。

### 生成(generation.py)
装了 `ragas` 用 RAGAS faithfulness / answer_relevancy;**没装走轻量自实现**(零额外依赖、可离线):
- **citation_coverage(引用覆盖率)** = 答案里出现的 `[n]` 引用编号中,真正指向召回 `sources`
  的比例。越界 / 虚构的引用编号(如只有 2 条 source 却写 `[3]`)会拉低该值 —— 近似 faithfulness。
- **cited_sentence_ratio(带引用句子比)** = 含 `[n]` 标记的句子数 / 句子总数 —— 近似「答案句子有无 source 支撑」。

> RAGAS 是重依赖(拉 langchain/datasets),放在 `[ragas]` optional extra,**不硬依赖**;
> `generation` 自动探测:装了标 `backend=ragas`,没装标 `backend=lightweight` 并用轻量指标兜底。

### 对账(reconcile.py)
对每个 `(tenant_id, source_table)`:
- **missing** = 源表有、`doc_vectors` 没有 → 漏 embed(正向漂移,通常该告警)。
- **orphan**  = `doc_vectors` 有、源表没有 → 源行删了但向量没清(残留)。

`in_sync` 当且仅当全部 missing + orphan 为 0。源表白名单:`article` / `product` / `comment`
(CDC 监听对象)。这是反查竞态 / 漏处理的兜底监控点。

## Golden set

`golden/queries.jsonl` 每行一条人工标注(基于 `db/init/01-init.sql` 样例数据):

```json
{"query": "pgvector 是什么", "tenant_id": "default",
 "expected_pks": [{"source_table": "article", "source_pk": "1"}]}
```

换数据集时新增 jsonl 行或用 `--golden <path>` 指向自定义 golden set。

## 测试

纯函数 + mock(不连真 rag / PG),验证指标算法本身正确:

```bash
cd eval
python -m pytest tests -q
```

覆盖:recall@k(截断 / 去重 / 类型归一)、MRR(首命中名次)、引用覆盖率(越界引用)、
对账漂移 diff(精确 set 模式 + 近似计数模式 + 多租户聚合)。
