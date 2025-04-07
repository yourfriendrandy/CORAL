from config.db_utils import connect_db
from config.dicts import SCHEMAS


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
    def __init__(self, table_name: str, metadata_scope: str = "local", duck: bool = True):
        self.table_name = table_name
        self.schema = SCHEMAS.get(table_name)
        if not self.schema:
            raise ValueError(f"No schema defined for `{table_name}`.")
        self.columns = [col.strip().split()[0] for col in self.schema.split(",")]
        self.duck = duck
        self.conn = connect_db(table_name, duck=duck, metadata_scope=metadata_scope)

    def insert_many(self, records: list[dict], conflict_strategy: str = "IGNORE"):
        if not records:
            return

        placeholders = ", ".join(["?"] * len(self.columns))
        col_str = ", ".join(self.columns)

        rows = []
        for record in records:
            row = tuple(record.get(col) for col in self.columns)
            if None in row and self.duck:
                raise ValueError(f"Missing required column(s) in record: {record}")
            rows.append(row)

        self.conn.execute("BEGIN TRANSACTION;")
        self.conn.executemany(
            f"INSERT OR {conflict_strategy.upper()} INTO {self.table_name} ({col_str}) VALUES ({placeholders})",
            rows
        )
        self.conn.commit()

    def close(self):
        self.conn.close()

# next classes: TextExportWriter for standarizing text output
# PipelineLogger for standardizing logging
