from config.db_utils import connect_db, insert_batch, setup_table
from config.dicts import SCHEMAS
from config.config import CORAL_TAG, TWIN_ONLY, PARTITION_MODE
from collections import OrderedDict
from hashlib import sha256
from pathlib import Path
import json


class PipelineStep:
    def __init__(self, name: str, logger_func=print):
        self.name = name
        self.logger = logger_func

    def __enter__(self):
        self.logger(f"🚀 Starting: {self.name}")
        return self

    def __exit__(self, exc_type, exc_val, _):
        if exc_type:
            self.logger(f"❌ Error in {self.name}: {exc_val}")
        else:
            self.logger(f"✅ Completed: {self.name}")

    def __call__(self, func):
        def wrapped(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return wrapped


class SchemaAwareWriter:
    def __init__(self, table_name: str, metadata_scope: str = "local", duck: bool = False):
        self.table_name = table_name
        self.schema = SCHEMAS.get(table_name)
        if not self.schema:
            raise ValueError(f"No schema defined for `{table_name}`.")
        self.columns = [col.strip().split()[0] for col in self.schema.split(",")]
        self.duck = duck
        self.conn = connect_db(table_name, duck=duck, metadata_scope=metadata_scope)

    def _filter_rows_by_mode(self, rows: list[tuple]) -> list[tuple]:
        try:
            t_index = self.columns.index("T")
            h_index = self.columns.index("H")
        except ValueError:
            raise ValueError("Columns 'T' and 'H' must be present for mode filtering.")

        def include(row):
            T, H = row[t_index], row[h_index]
            return (T == H + 1 if TWIN_ONLY else T != H + 1 if PARTITION_MODE else True)

        return [row for row in rows if include(row)]

    def write_rows(self, rows: list, mode_filtered: bool = False):
        if not rows:
            return

        if isinstance(rows[0], dict):
            data = [tuple(row.get(col) for col in self.columns) for row in rows]
        else:
            data = rows

        if mode_filtered:
            data = self._filter_rows_by_mode(data)

        placeholders = ", ".join(["?"] * len(self.columns))
        col_str = ", ".join(self.columns)

        self.conn.execute("BEGIN TRANSACTION;")
        self.conn.executemany(
            f"INSERT OR IGNORE INTO {self.table_name} ({col_str}) VALUES ({placeholders})",
            data
        )
        self.conn.commit()

    def write_json_rows(self, pairs: list[tuple]):
        json_rows = [(n, json.dumps(seq)) for n, seq in pairs]
        self.write_rows(json_rows)

    def close(self):
        self.conn.close()


class LoopCache:
    def __init__(self, cache_size=50, use_duck: bool = True):
        self.table = "infinite_loops"
        if use_duck:
            setup_table(self.table)  # Only creates DuckDB table if needed
        self.conn = connect_db(self.table, duck=False)  # Always use SQLite for writing
        self.cache = OrderedDict()
        self.capacity = cache_size

    def _normalize_loop(self, loop):
        return min(tuple(loop[i:] + loop[:i]) for i in range(len(loop)))

    def _hash_loop(self, norm_loop):
        return sha256(",".join(map(str, norm_loop)).encode()).hexdigest()

    def contains(self, loop):
        norm_loop = self._normalize_loop(loop)
        h = self._hash_loop(norm_loop)
        if h in self.cache:
            self.cache.move_to_end(h)
            return True
        cur = self.conn.cursor()
        cur.execute(f"SELECT 1 FROM {self.table} WHERE normalized_hash = ?", (h,))
        if cur.fetchone():
            self.cache[h] = True
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)
            return True
        return False

    def add_loop(self, loop, full_sequence):
        norm_loop = self._normalize_loop(loop)
        h = self._hash_loop(norm_loop)
        if self.contains(loop):
            return False
        insert_batch(
            self.conn,
            self.table,
            [(h, json.dumps(norm_loop), json.dumps(full_sequence))],
            "(normalized_hash, loop, sequence)"
        )
        self.cache[h] = True
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)
        return True

    def _get_entry_point(self, full_seq, loop):
        try:
            i = full_seq.index(loop[0])
            return full_seq[i - 1] if i > 0 else loop[0]
        except ValueError:
            return None

    def export_all(self, output_path: str, system_tag: str):
        Path(output_path).parent.mkdir(exist_ok=True)
        with self.conn as conn, open(output_path, "a") as f:
            f.write(f"\n♾️ Infinite Loops for {system_tag}:\n\n")
            rows = conn.execute(f"SELECT loop, sequence FROM {self.table}").fetchall()
            for raw_loop, sequence in rows:
                loop = json.loads(raw_loop)
                full_seq = json.loads(sequence)
                entry = self._get_entry_point(full_seq, loop)
                f.write(f"Loop: {loop}\n")
                if entry is not None:
                    f.write(f" - Entry point: {entry}\n")
                f.write("\n")

    def close(self):
        self.conn.close()


class DiskLoopTracker(LoopCache):
    def __init__(self, cache_size=50, use_duck: bool = True):
        super().__init__(cache_size=cache_size, use_duck=use_duck)
        self.entry_table = "loop_entry_points"
        if use_duck:
            setup_table(self.entry_table)
        self.entry_conn = connect_db(self.entry_table, duck=False)

    def _entry_exists(self, entry, loop):
        cur = self.entry_conn.cursor()
        cur.execute(
            f"SELECT 1 FROM {self.entry_table} WHERE integer = ? AND loop = ?",
            (entry, json.dumps(loop))
        )
        return cur.fetchone() is not None

    def add_loop_with_entry(self, loop, full_sequence, source_n):
        added = self.add_loop(loop, full_sequence)
        if not added:
            return False
        entry = self._get_entry_point(full_sequence, loop)
        if entry is not None and not self._entry_exists(entry, loop):
            insert_batch(
                self.entry_conn,
                self.entry_table,
                [(entry, json.dumps(loop))],
                "(integer, loop)"
            )
        return True

    def get_all_entry_points(self):
        cur = self.entry_conn.cursor()
        cur.execute(f"SELECT DISTINCT integer FROM {self.entry_table}")
        return {row[0] for row in cur.fetchall()}

    def close(self):
        super().close()
        self.entry_conn.close()