#!/usr/bin/env python3
import json
import ray

from config.config import INTEGER_MAX, print_active_mode
from config.logger import logger
from config.ray_utils import run_ray_futures
from config.class_utils import SchemaAwareWriter, PipelineStep
from config.db_utils import (
    read_duckdb,
    read_resume_checkpoint,
    write_resume_checkpoint,
    port_and_prune_sqlite_to_duckdb,
)
from config.path_utils import get_path

TABLE_NAME = "integer_cycles"

# ----------------------------------
# Ray Task: Fetch Collatz Sequences
# ----------------------------------
@ray.remote
def fetch_cycles_remote(chunk):
    from config.db_utils import get_db_paths
    import duckdb
    import json

    _, duckdb_path = get_db_paths("system_cache")  # Resolves to {CORAL_TAG}_sequences.duckdb
    conn = duckdb.connect(str(duckdb_path), read_only=True)
    placeholders = ",".join("?" for _ in chunk)
    query = f"""
        SELECT n, sequence FROM system_cache
        WHERE n IN ({placeholders})
    """
    cursor = conn.execute(query, chunk)
    results = {n: json.loads(seq) for n, seq in cursor.fetchall()}
    conn.close()
    return results

# ----------------------------------
# Main Driver
# ----------------------------------
@PipelineStep("Generate motif integer cycles", logger.info)
def process_motif_cycles():
    print_active_mode()
    ray.init(ignore_reinit_error=True)

    # 🛠️ Writer only (tables already set up globally)
    writer = SchemaAwareWriter(TABLE_NAME)

    # 🔁 Resume support
    try:
        last_seen = read_resume_checkpoint(TABLE_NAME)
        logger.info(f"🔁 Resuming from motif_integer = {last_seen + 1}")
    except FileNotFoundError:
        last_seen = -1

    # 📥 Load motif integers from DuckDB with mode filtering
    raw_rows = read_duckdb(
        table_name="integers",
        where_clause=f"integer_An > {last_seen}" if last_seen >= 0 else "",
        as_dict=True,
        filter_by_mode=True,
    )
    motif_dict = {row["integer_An"]: row for row in raw_rows}
    logger.info(f"🔍 {len(motif_dict)} motif integers retrieved")

    if not motif_dict:
        logger.info("✅ All motif integers already processed.")
        return

    # 🧠 Chunking
    def chunkify(lst, size):
        return [lst[i:i + size] for i in range(0, len(lst), size)]

    chunked = chunkify(list(motif_dict.keys()), size=500)

    def store_callback(result_dict):
        rows = [
            (n, row["H"], row["T"], json.dumps(seq))
            for n, seq in result_dict.items()
            if (row := motif_dict.get(n))
        ]
        writer.write_rows(rows, mode_filtered=True)

    # 🚀 Parallel collection
    run_ray_futures(
        chunked_data=chunked,
        remote_function=fetch_cycles_remote,
        store_callback=store_callback,
        log_prefix="Motif Cycle",
    )

    # 🧼 Finalize + checkpoint
    last_key = port_and_prune_sqlite_to_duckdb(TABLE_NAME)
    write_resume_checkpoint(TABLE_NAME, last_key)

    # 📄 Output message
    output_path = get_path("text_output", "tagged", TABLE_NAME + "_output.txt")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        f.write(f"✅ Motif integer cycles written to `{TABLE_NAME}` for up to n = {INTEGER_MAX}\n")

    logger.info("🎉 Motif cycle generation complete.")

# Entrypoint
if __name__ == "__main__":
    process_motif_cycles()
    ray.shutdown()