from __future__ import annotations

import json
from pathlib import Path

from br_market.settings import (
    AWS_CONN_ID,
    POSTGRES_CONN_ID,
    S3_BUCKET,
    s3_local_dir,
    store_dir,
)


def _key(row: dict, key_fields: tuple[str, ...]) -> tuple:
    return tuple(row[field] for field in key_fields)


def upsert_records(
    table: str,
    rows: list[dict],
    columns: tuple[str, ...],
    key_fields: tuple[str, ...],
    postgres_conn_id: str = POSTGRES_CONN_ID,
) -> int:
    destination = store_dir()
    if destination is not None:
        destination.mkdir(parents=True, exist_ok=True)
        path = destination / f"{table.replace('.', '_')}.json"
        existing: list[dict] = []
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
        index = {_key(row, key_fields): i for i, row in enumerate(existing)}
        for row in rows:
            keyed = _key(row, key_fields)
            if keyed in index:
                existing[index[keyed]] = row
            else:
                index[keyed] = len(existing)
                existing.append(row)
        path.write_text(json.dumps(existing, indent=2, sort_keys=True), encoding="utf-8")
        return len(rows)

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    tuples = [tuple(row[column] for column in columns) for row in rows]
    hook.insert_rows(
        table=table,
        rows=tuples,
        target_fields=list(columns),
        replace=True,
        replace_index=list(key_fields),
        fast_executemany=True,
    )
    return len(rows)


def fetch_records(postgres_conn_id: str, sql: str, table: str | None = None) -> list[dict]:
    destination = store_dir()
    if destination is not None and table:
        path = destination / f"{table.replace('.', '_')}.json"
        if not path.exists():
            return []
        return json.loads(path.read_text(encoding="utf-8"))

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    records = hook.get_records(sql)
    return records


def publish_json(key: str, payload: str) -> str:
    local = s3_local_dir()
    if local is not None:
        path = Path(local) / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
        return str(path)

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    hook.load_string(string_data=payload, key=key, bucket_name=S3_BUCKET, replace=True)
    return f"s3://{S3_BUCKET}/{key}"
