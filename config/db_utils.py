import sqlite3
import duckdb
import pandas as pd
import json
from pathlib import Path
from config.config import CORAL_TAG, TWIN_ONLY, PARTITION_MODE
from config.dicts import DB_MAP, DB_TAG_MAP, SCHEMAS

# ----------------------------
# PATH + SCHEMA HELPERS
# ----------------------------


def get_project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def get_db_info(table: str) -> dict:
    if table in DB_TAG_MAP:
        return DB_TAG_MAP[table]
    if table in DB_MAP:
        return DB_MAP[table]
    raise ValueError(f"Unknown table: {table}")


def get_db_paths(table: str, scope: str = "local") -> tuple[Path, Path]:
    info = get_db_info(table)
    folder = (
        get_project_root() / "databases" / "all_systems_metadata" / scope
        if info["is_metadata"]
        else get_project_root() / "databases" / CORAL_TAG
    )
    folder.mkdir(parents=True, exist_ok=True)
    base = info["db_name"]
    sqlite = folder / f"{base}_temp.db"
    duck = folder / f"{(CORAL_TAG + '_') if table in DB_TAG_MAP else ''}{base}.duckdb"
    return sqlite, duck


def strip_constraints(schema: str) -> str:
    """
    Removes constraint keywords like PRIMARY KEY, UNIQUE, NOT NULL
    but keeps valid 'col TYPE' declarations for DuckDB.
    """
    out = []
    for col in schema.split(","):
        parts = col.strip().split()
        if len(parts) >= 2:
            col_name, col_type = parts[0], parts[1]
            if col_type.upper() in {"PRIMARY", "UNIQUE", "NOT", "KEY"}:
                continue
            out.append(f"{col_name} {col_type}")
    return ", ".join(out)


def get_primary_key(schema: str) -> str:
    for part in schema.split(","):
        if "PRIMARY KEY" in part.upper():
            return part.strip().split()[0]
    raise ValueError("No PRIMARY KEY found in schema.")


def connect_db(table_name: str, duck: bool = False, metadata_scope: str = "local"):
    """
    Returns a connection to either SQLite or DuckDB for the specified table.
    """
    sqlite_path, duckdb_path = get_db_paths(table_name, metadata_scope)
    return duckdb.connect(duckdb_path) if duck else sqlite3.connect(sqlite_path)

# ----------------------------
# SETUP LOGIC
# ----------------------------


def setup_table(table: str, scope: str = "local"):
    schema = SCHEMAS.get(table)
    if not schema:
        raise ValueError(f"No schema for {table}")

    sqlite_path, duck_path = get_db_paths(table, scope)

    with sqlite3.connect(sqlite_path) as s_conn:
        s_conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({schema})")

    with duckdb.connect(duck_path) as d_conn:
        d_conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({strip_constraints(schema)})")


def setup_all_tables(scope: str = "local"):
    for table in DB_MAP | DB_TAG_MAP:
        setup_table(table, scope)

# ----------------------------
# INSERTS
# ----------------------------


def insert_batch(conn, table: str, data: list[tuple], columns: str, conflict: str = "IGNORE"):
    if not data:
        return
    placeholders = ", ".join(["?"] * len(data[0]))
    conn.execute("PRAGMA synchronous = OFF;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.executemany(
        f"INSERT OR {conflict.upper()} INTO {table} {columns} VALUES ({placeholders})", data
    )
    conn.commit()

# ----------------------------
# PORTING + CHECKPOINTS
# ----------------------------


def port_and_prune_sqlite_to_duckdb(table: str, scope: str = "local") -> int:
    sqlite_path, duckdb_path = get_db_paths(table, scope)
    schema = SCHEMAS[table]
    key = get_primary_key(schema)

    with sqlite3.connect(sqlite_path) as s_conn, duckdb.connect(duckdb_path) as d_conn:
        max_val = s_conn.execute(f"SELECT MAX({key}) FROM {table}").fetchone()[0]
        if max_val is None:
            return -1

        df = pd.read_sql_query(
            f"SELECT * FROM {table} WHERE {key} < ? ORDER BY {key}",
            s_conn, params=(max_val,)
        )
        if df.empty:
            return max_val

        d_conn.execute(f"CREATE TABLE IF NOT EXISTS {table} ({strip_constraints(schema)})")
        d_conn.register("temp_df", df)
        d_conn.execute(f"""
            INSERT INTO {table}
            SELECT * FROM temp_df
            WHERE {key} NOT IN (SELECT {key} FROM {table})
        """)
        s_conn.execute(f"DELETE FROM {table} WHERE {key} < ?", (max_val,))
        s_conn.commit()
        return max_val


def write_resume_checkpoint(table: str, last_key: int, checkpoint_dir: Path = Path("checkpoints")) -> Path:
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    path = checkpoint_dir / f"{table}.json"
    with open(path, "w") as f:
        json.dump({"last_synced_key": last_key}, f, indent=2)
    return path


def read_resume_checkpoint(table: str, checkpoint_dir: Path = Path("checkpoints")) -> int:
    path = checkpoint_dir / f"{table}.json"
    if not path.exists():
        raise FileNotFoundError(f"❌ Missing checkpoint for table: {table} at {path}")
    with open(path) as f:
        return json.load(f).get("last_synced_key", -1)
        

# ----------------------------
# DUCK READ TOOL
# ----------------------------


def read_duckdb(
    table_name: str,
    where_clause: str = "",
    as_dict: bool = False,
    metadata_scope: str = "local",
    filter_by_mode: bool = False
):
    """
    Basic DuckDB read with optional WHERE, dict output, and mode-aware filtering (TWIN_ONLY / PARTITION_MODE).
    Always opens the connection in read-only mode to support Ray parallelization safely.
    """
    _, duckdb_path = get_db_paths(table_name, metadata_scope)
    base_query = f"SELECT * FROM {table_name}"

    # Add T/H mode clause if requested
    mode_clause = ""
    if filter_by_mode:
        if TWIN_ONLY:
            mode_clause = "T = H + 1"
        elif PARTITION_MODE:
            mode_clause = "T != H + 1"

    # Combine clauses
    if where_clause and mode_clause:
        full_where = f"{where_clause} AND {mode_clause}"
    elif where_clause:
        full_where = where_clause
    else:
        full_where = mode_clause

    if full_where:
        base_query += f" WHERE {full_where}"

    # Always open connection in read-only mode
    with duckdb.connect(str(duckdb_path), read_only=True) as conn:
        cursor = conn.execute(base_query)
        if as_dict:
            cols = [desc[0] for desc in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
        return cursor.fetchall()