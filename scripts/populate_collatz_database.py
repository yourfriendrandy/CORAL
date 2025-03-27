#!/usr/bin/env python3
import os
import ray
import sqlite3
import json
from config.config import INTEGER_LIMIT, CHUNK_SIZE, print_active_mode
from config.db_utils import setup_collatz_db, connect_db

@ray.remote
def compute_collatz_batch(chunk):
    def collatz_sequence(n):
        sequence = []
        while n > 1:
            sequence.append(n)
            n = n // 2 if n % 2 == 0 else 3 * n + 1
        sequence.append(1)
        return sequence

    return [(n, collatz_sequence(n)) for n in chunk]

def get_existing_numbers():
    """Returns a set of all numbers currently stored in the database."""
    conn = connect_db("collatz_sequences.db")
    cursor = conn.cursor()
    cursor.execute("SELECT n FROM collatz_cache WHERE n <= ?", (INTEGER_LIMIT,))
    existing = set(row[0] for row in cursor.fetchall())
    conn.close()
    return existing

def store_sequences_in_db(sequences):
    """Stores a list of (n, sequence) pairs into collatz_cache with optimized batching."""
    conn = connect_db("collatz_sequences.db")
    cursor = conn.cursor()

    # Optimized SQLite performance settings
    cursor.execute("PRAGMA synchronous = OFF;")
    cursor.execute("PRAGMA journal_mode = WAL;")

    cursor.execute("BEGIN TRANSACTION;")
    cursor.executemany(
        'INSERT OR REPLACE INTO collatz_cache (n, sequence) VALUES (?, ?)',
        [(n, json.dumps(seq)) for n, seq in sequences]
    )
    conn.commit()
    conn.close()

def populate_collatz_db_ray():
    print_active_mode()
    ray.init(ignore_reinit_error=True, num_cpus=os.cpu_count() - 1)
    setup_collatz_db()

    existing = get_existing_numbers()
    missing = [n for n in range(1, INTEGER_LIMIT + 1) if n not in existing]
    
    if not missing:
        print(f"✅ All sequences up to {INTEGER_LIMIT} already exist in the database.")
        return

    # Optional: adaptive chunk sizing (commented out for now)
    # chunks = []
    # for i in range(0, len(missing), CHUNK_SIZE):
    #     scale = min(CHUNK_SIZE, 1000 + (i // 500_000) * 500)
    #     chunks.append(missing[i:i + scale])
    
    # Standard chunking
    chunks = [missing[i:i + CHUNK_SIZE] for i in range(0, len(missing), CHUNK_SIZE)]

    print(f"🚀 Starting Ray job for {len(missing)} missing numbers in {len(chunks)} chunks...")

    futures = [compute_collatz_batch.remote(chunk) for chunk in chunks]

    for i, result_batch in enumerate(ray.get(futures)):
        store_sequences_in_db(result_batch)
        if (i + 1) % 10 == 0 or (i + 1) == len(futures):
            print(f"✅ Stored chunk {i + 1}/{len(futures)}")

    print(f"🎉 All missing Collatz sequences stored in `collatz_sequences.db` up to {INTEGER_LIMIT}.")

if __name__ == "__main__":
    populate_collatz_db_ray()
