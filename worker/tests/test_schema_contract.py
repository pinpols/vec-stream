import ast
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "worker" / "src"))

from vec_stream_worker.config import DEFAULT_TABLES  # noqa: E402

SOURCE_TABLES = {"article", "product", "comment"}


def _source_columns() -> dict[str, set[str]]:
    sql = (ROOT / "db" / "init" / "01-init.sql").read_text()
    out: dict[str, set[str]] = {}
    for table, body in re.findall(
        r"CREATE TABLE IF NOT EXISTS\s+([a-z_]+)\s*\((.*?)\);",
        sql,
        flags=re.S,
    ):
        if table not in SOURCE_TABLES:
            continue
        cols: set[str] = set()
        for raw_line in body.splitlines():
            line = raw_line.strip().rstrip(",")
            if not line or line.startswith("--"):
                continue
            token = line.split()[0].lower()
            if token not in {"primary", "foreign", "unique", "constraint", "check"}:
                cols.add(token)
        out[table] = cols
    return out


def _lake_columns(path: str) -> dict[str, list[str]]:
    tree = ast.parse((ROOT / path).read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "TABLES" for t in node.targets
        ):
            out: dict[str, list[str]] = {}
            for key, value in zip(node.value.keys, node.value.values, strict=True):
                table = key.value
                out[table] = [item.elts[0].value for item in value.elts]
            return out
    raise AssertionError(f"TABLES not found in {path}")


def test_worker_table_config_matches_source_schema():
    source = _source_columns()

    assert set(DEFAULT_TABLES) == SOURCE_TABLES
    for table, cfg in DEFAULT_TABLES.items():
        columns = source[table]
        parent = cfg.get("reembed_parent")
        if parent:
            assert parent["table"] in source
            assert parent["fk"] in columns
            assert DEFAULT_TABLES[parent["table"]]["pk"] in source[parent["table"]]
            continue

        assert cfg["pk"] in columns
        assert cfg["title_field"] in columns
        assert set(cfg["fields"]).issubset(columns)


def test_lakehouse_schema_matches_source_schema():
    source = _source_columns()
    hudi = _lake_columns("spark-lake/cdc_to_hudi.py")
    iceberg = _lake_columns("spark-lake/cdc_to_iceberg.py")

    assert set(hudi) == SOURCE_TABLES
    assert set(iceberg) == SOURCE_TABLES
    for table in SOURCE_TABLES:
        assert hudi[table] == iceberg[table]
        assert {"id", "tenant_id"}.issubset(hudi[table])
        assert set(hudi[table]).issubset(source[table])
