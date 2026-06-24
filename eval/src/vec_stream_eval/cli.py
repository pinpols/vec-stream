"""命令行入口:python -m vec_stream_eval retrieval|generation|reconcile。"""
from __future__ import annotations

import argparse
import sys

from . import generation, reconcile, retrieval


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="vec-stream-eval",
        description="vec_stream RAG 评估:检索质量 / 生成质量 / 一致性对账",
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # retrieval
    p_ret = sub.add_parser("retrieval", help="检索质量 recall@k / MRR(打真 /search)")
    p_ret.add_argument("--golden", default=None, help="golden jsonl 路径(默认 eval/golden/queries.jsonl)")
    p_ret.add_argument("--rag-url", default=None, help="rag 服务地址(默认 env RAG_URL 或 http://localhost:8000)")
    p_ret.add_argument("-k", type=int, default=5, help="recall@k 与召回 top_k(默认 5)")
    p_ret.add_argument("--rerank", action="store_true", help="开启服务端 rerank")
    p_ret.add_argument("--json-out", default=None, help="把报告写到 JSON 文件")

    # generation
    p_gen = sub.add_parser("generation", help="生成质量 faithfulness / 引用覆盖率(打真 /ask)")
    p_gen.add_argument("--golden", default=None)
    p_gen.add_argument("--rag-url", default=None)
    p_gen.add_argument("--json-out", default=None)

    # reconcile
    p_rec = sub.add_parser("reconcile", help="一致性对账:doc_vectors vs 源表行数(只读连 PG)")
    p_rec.add_argument("--dsn", default=None, help="PG DSN(默认 env RAG_PG_DSN / PG_DSN)")
    p_rec.add_argument("--json-out", default=None)

    args = parser.parse_args(argv)

    if args.cmd == "retrieval":
        retrieval.run(
            golden_path=args.golden, rag_url=args.rag_url,
            k=args.k, rerank=args.rerank, json_out=args.json_out,
        )
    elif args.cmd == "generation":
        generation.run(golden_path=args.golden, rag_url=args.rag_url, json_out=args.json_out)
    elif args.cmd == "reconcile":
        reconcile.run(dsn=args.dsn, json_out=args.json_out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
