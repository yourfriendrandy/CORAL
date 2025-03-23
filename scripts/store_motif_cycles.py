#!/usr/bin/env python3
import os
import json
import ray

from config.config import TEXT_OUTPUT_PATH, INTEGER_LIMIT, TWIN_ONLY, DB_PATH
from config.db_utils import connect_db, setup_motif_cycles_db

# Initialize Ray
ray.init(ignore_reinit_error=True, num_cpus=os.cpu_count() - 1)

BATCH_SIZE = 1000

def fetch_motif_integers(limit):
    """Fetches integers from motif_integers.db (filtered by motif_parameters if TWIN_ONLY)."""
    conn = connect_db("motif_integers.db")
    cursor = conn.cursor()

    if TWIN_ONLY:
        # Attach motif_parameters.db so we can filter by B = Y + 1
        cursor.execute(f"ATTACH DATABASE ? AS params", (os.path.join(DB_PATH, "motif_parameters.db"),))
        cursor.execute("""
            SELECT m.integer_N
            FROM motif_integers m
            JOIN params.motif_parameters p ON m.motif_A = p.A
            WHERE p.B = p.Y + 1 AND m.integer_N <= ?
        """, (limit,))
    else:
        cursor.execute("SELECT integer_N FROM motif_integers WHERE integer_N <= ?", (limit,))

    integers = [row[0] for row in cursor.fetchall()]
    conn.close()
    return integers

@ray.remote
def fetch_collatz_cycles_chunk(chunk):
    """Fetches a chunk of Collatz sequences."""
    conn = connect_db("collatz_sequences.db")
    cursor = conn.cursor()
    query = f"SELECT n, sequence FROM collatz_cache WHERE n IN ({','.join('?' * len(chunk))})"
    cursor.execute(query, chunk)
    result = {row[0]: json.loads(row[1]) for row in cursor.fetchall()}
    conn.close()
    return result

def store_motif_cycles_bulk(motif_cycles):
    """Stores motif cycles into the motif_cycles.db."""
    conn = connect_db("motif_cycles.db")
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT OR IGNORE INTO motif_cycles (motif_integer, cycle)
        VALUES (?, ?)
    ''', [(n, json.dumps(seq)) for n, seq in motif_cycles.items()])
    conn.commit()
    conn.close()

def process_motif_cycles_ray():
    print("⚡ Initializing motif cycle generation using Ray...")
    setup_motif_cycles_db()

    motif_integers = fetch_motif_integers(INTEGER_LIMIT)
    chunks = [motif_integers[i:i + BATCH_SIZE] for i in range(0, len(motif_integers), BATCH_SIZE)]

    print(f"🔍 Total motif integers: {len(motif_integers)} split into {len(chunks)} chunks.")

    futures = [fetch_collatz_cycles_chunk.remote(chunk) for chunk in chunks]

    output_file = os.path.join(TEXT_OUTPUT_PATH, "motif_cycles_output.txt")
    missing_motifs = []

    with open(output_file, "w") as file:
        file.write("### Motif Cycles Output ###\n\n")

        for chunk, future in zip(chunks, futures):
            result = ray.get(future)
            store_motif_cycles_bulk(result)

            for n in result:
                file.write(f"{n}: {result[n]}\n")

            # Detect missing cycles
            chunk_missing = [n for n in chunk if n not in result]
            if chunk_missing:
                missing_motifs.extend(chunk_missing)

        if missing_motifs:
            file.write("\n### Missing Collatz Cycles (Not Found in collatz_sequences.db) ###\n")
            file.write(", ".join(map(str, missing_motifs)) + "\n")

    print(f"✅ Step 4 complete. Cycles saved to `motif_cycles.db` and '{output_file}'.")

if __name__ == "__main__":
    process_motif_cycles_ray()
