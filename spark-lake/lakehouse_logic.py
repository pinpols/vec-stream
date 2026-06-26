"""Small pure helpers shared by spark-lake scripts and local tests."""

from __future__ import annotations

EVENT_POS_FORMAT = "%020d:%020d"


def format_event_pos(lsn_or_ts: int | None, offset: int) -> str:
    return EVENT_POS_FORMAT % (int(lsn_or_ts or 0), int(offset))


def normalize_hudi_record_key(value: str | None) -> str:
    return (value or "").replace(" ", "").strip("[]")


def parse_query_key_args(args: list[str]) -> tuple[str, str | None]:
    tenant_id = "default"
    pk = None
    if len(args) == 1:
        pk = args[0]
    elif len(args) >= 2:
        tenant_id = args[0]
        pk = args[1]
    return tenant_id, pk
