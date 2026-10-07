from __future__ import annotations

import os
from pathlib import Path

import duckdb

from .normalise import normalise_course, normalise_horse


def get_db(path: str | Path) -> duckdb.DuckDBPyConnection:
    db = duckdb.connect(str(path))
    threads = os.environ.get("RACING_DUCKDB_THREADS")
    if threads:
        db.execute("SET threads = ?", [int(threads)])
    memory_limit = os.environ.get("RACING_DUCKDB_MEMORY_LIMIT")
    if memory_limit:
        db.execute("SET memory_limit = ?", [memory_limit])
    temp_directory = os.environ.get("RACING_DUCKDB_TEMP_DIRECTORY")
    if temp_directory:
        db.execute("SET temp_directory = ?", [temp_directory])
    db.execute("SET preserve_insertion_order = false")
    try:
        db.create_function("normalise_course", normalise_course, [str], str)
    except duckdb.CatalogException:
        pass
    try:
        db.create_function("normalise_horse", normalise_horse, [str], str)
    except duckdb.CatalogException:
        pass
    return db
