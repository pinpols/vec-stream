"""Golden set 加载与数据模型。

golden/queries.jsonl 每行一条人工标注:
  {"query": "...", "tenant_id": "default",
   "expected_pks": [{"source_table": "article", "source_pk": "1"}, ...]}
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, Field, field_validator

# 仓库内默认 golden set 路径(eval/golden/queries.jsonl)
DEFAULT_GOLDEN = Path(__file__).resolve().parents[2] / "golden" / "queries.jsonl"


class DocRef(BaseModel):
    """一个期望被召回的源文档引用(表 + 主键)。"""

    source_table: str
    source_pk: str

    @field_validator("source_pk", mode="before")
    @classmethod
    def _coerce_pk(cls, v):
        # golden 标注里主键常写成数字(JSON number);doc_vectors.source_pk 是 TEXT,统一成字符串。
        return str(v)

    def key(self) -> tuple[str, str]:
        # 主键统一转字符串比较:doc_vectors.source_pk 是 TEXT,golden 也可能写成数字。
        return (self.source_table, str(self.source_pk))


class GoldenQuery(BaseModel):
    query: str
    tenant_id: str = "default"
    expected_pks: list[DocRef] = Field(default_factory=list)

    def expected_keys(self) -> set[tuple[str, str]]:
        return {d.key() for d in self.expected_pks}


def load_golden(path: str | Path | None = None) -> list[GoldenQuery]:
    """读取 jsonl golden set。空行跳过,非法行立即报错(标注质量要 fail-fast)。"""
    p = Path(path) if path else DEFAULT_GOLDEN
    if not p.exists():
        raise FileNotFoundError(f"golden set 不存在: {p}")
    out: list[GoldenQuery] = []
    for i, line in enumerate(p.read_text(encoding="utf-8").splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            out.append(GoldenQuery.model_validate(json.loads(line)))
        except Exception as e:  # noqa: BLE001
            raise ValueError(f"golden set 第 {i} 行解析失败: {e}") from e
    if not out:
        raise ValueError(f"golden set 为空: {p}")
    return out
