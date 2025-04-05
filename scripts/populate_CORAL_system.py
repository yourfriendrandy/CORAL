#!/usr/bin/env python3
import os
import ray
import json
from pathlib import Path
from config.config import (
    INTEGER_LIMIT,
    CHUNK_SIZE,
    PORT_BATCH_SIZE,
    print_active_mode,
    M,
    Z,
    CORAL_TAG
)
from config.logger import logger
from config.db_utils import (
    setup_CORAL_db,
    get_temp_sqlite_path,
    get_duckdb_path,
    port_and_prune_sqlite_to_duckdb,
    get_last_sqlite_entry,
    optimized_batch_insert,
    connect_db,
    ensure_temp_table_exists_from_duck_schema
)
from config.ray_utils import stream_ray_batches, init_ray

# === CORAL Sequence Generator ===
def generate_CORAL_sequence(n, m, z):
    seq = [n]
    while n > 1:
        if n % z == 0:
            n //= z
        else:
            for b in range(1, z):
                if (m * n + b) % z == 0:
                    n = (m * n + b) // z
                    break
        seq.append(n)
    return seq

@ray.remote
def compute_CORAL_batch(chunk, m, z):
    return [(n, generate_CORAL_sequence(n, m, z)) for n in chunk]

# === Writer ===
def store_sequences_in_db(sequences):
    conn = connect_db(get_temp_sqlite_path())
    data = [(n, json.dumps(seq)) for n, seq in sequences]
    optimized_batch_insert(conn, "system_cache", data, "(n, sequence)")
    conn.close()

# === Main Driver ===
def populate_coral_db_ray():
    print_active_mode()
    init_ray()

    logger.info(f"🧪 Initializing CORAL system for M={M}, Z={Z} → {CORAL_TAG}")
    setup_CORAL_db()

    ensure_temp_table_exists_from_duck_schema("system_cache")

    temp_path = get_temp_sqlite_path()
    duck_path = get_duckdb_path()

    last_n = get_last_sqlite_entry(temp_path, table="system_cache")
    start = last_n + 1 if last_n else 1
    end = 3 * INTEGER_LIMIT

    full_range = list(range(start, end + 1))
    chunks = [full_range[i:i + CHUNK_SIZE] for i in range(0, len(full_range), CHUNK_SIZE)]

    logger.info(f"📈 Starting from n = {start}, computing up to {end}")
    logger.info(f"🧠 Porting every {PORT_BATCH_SIZE} batches")

    def port_callback():
        retained = port_and_prune_sqlite_to_duckdb(
            sqlite_path=temp_path,
            duckdb_path=duck_path,
            table_name="system_cache",
            key_column="n",
            logger=logger,
            delete_sqlite=False
        )
        logger.info(f"📦 Ported to DuckDB. Retained n = {retained}")

    stream_ray_batches(
        chunked_data=chunks,
        remote_function=lambda chunk: compute_CORAL_batch.remote(chunk, M, Z),
        store_callback=store_sequences_in_db,
        max_in_flight=4,
        port_every=PORT_BATCH_SIZE,
        port_callback=port_callback,
        log_prefix="CORAL chunk"
    )

    # Final port and delete temp DB
    port_and_prune_sqlite_to_duckdb(
        sqlite_path=temp_path,
        duckdb_path=duck_path,
        table_name="system_cache",
        logger=logger,
        delete_sqlite=True
    )

    logger.info(f"🎉 CORAL sequence generation complete for {CORAL_TAG} up to n = {end}")

if __name__ == "__main__":
    populate_coral_db_ray()
    ray.shutdown()
