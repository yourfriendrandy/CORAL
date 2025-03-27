#!/usr/bin/env python3
import os
import json
import ray

from config.config import (
    TEXT_OUTPUT_PATH,
    INTEGER_LIMIT,
    TWIN_ONLY,
    PARTITION_MODE,
    print_active_mode
)
from config.db_utils import connect_db, setup_motif_cycles_db, get_project_root

# Initialize Ray
ray.init(ignore_reinit_error=True, num_cpus=os.cpu_count() - 1)

BATCH_SIZE = 1000

def get_existing_motif_cycles():
    conn = connect_db("motif_cycles.db")
    cursor = conn.cursor()
    cursor.execute("SELECT motif_integer FROM motif_cycles")
    existing = {row[0] for row in cursor.fetchall()}
    conn.close()
    return existing

def fetch_motif_integers(limit, exclude_set):
    conn = connect_db("motif_cycles.db")
    cursor = conn.cursor()

    root = get_project_root()
    motif_params_path = os.path.join(root, "databases", "motif_parameters.db")
    motif_integers_path = os.path.join(root, "databases", "motif_integers.db")

    cursor.execute("ATTACH DATABASE ? AS mi", (motif_integers_path,))
    cursor.execute("ATTACH DATABASE ? AS params", (motif_params_path,))

    if TWIN_ONLY:
        filter_clause = "AND p.B = p.Y + 1"
    elif PARTITION_MODE:
        filter_clause = "AND p.B != p.Y + 1"
    else:
        filter_clause = ""

    query = f"""
        SELECT m.integer_N
        FROM mi.motif_integers m
        JOIN params.motif_parameters p ON m.motif_A = p.A
        WHERE m.integer_N <= ?
        {filter_clause}
    """

    cursor.execute(query, (limit,))
    all_ints = [row[0] for row in cursor.fetchall()]
    conn.close()

    return [n for n in all_ints if n not in exclude_set]

@ray.remote
def fetch_collatz_cycles_chunk(chunk):
    conn = connect_db("collatz_sequences.db")
    cursor = conn.cursor()
    query = f"SELECT n, sequence FROM collatz_cache WHERE n IN ({','.join('?' * len(chunk))})"
    cursor.execute(query, chunk)
    result = {row[0]: json.loads(row[1]) for row in cursor.fetchall()}
    conn.close()
    return result

def store_motif_cycles_bulk(motif_cycles):
    conn = connect_db("motif_cycles.db")
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT OR IGNORE INTO motif_cycles (motif_integer, cycle)
        VALUES (?, ?)
    ''', [(n, json.dumps(seq)) for n, seq in motif_cycles.items()])
    conn.commit()
    conn.close()

def process_motif_cycles_ray():
    print_active_mode()
    print("⚡ Initializing motif cycle generation using Ray...")
    setup_motif_cycles_db()

    already_processed = get_existing_motif_cycles()
    motif_integers = fetch_motif_integers(INTEGER_LIMIT, already_processed)
    chunks = [motif_integers[i:i + BATCH_SIZE] for i in range(0, len(motif_integers), BATCH_SIZE)]

    print(f"🔍 Found {len(motif_integers)} unprocessed motif integers, split into {len(chunks)} chunks.")

    futures = [fetch_collatz_cycles_chunk.remote(chunk) for chunk in chunks]

    os.makedirs(TEXT_OUTPUT_PATH, exist_ok=True)
    output_file = os.path.join(TEXT_OUTPUT_PATH, "motif_cycles_output.txt")
    missing_motifs = []

    with open(output_file, "w") as file:
        file.write("### Motif Cycles Output ###\n\n")

        for chunk, future in zip(chunks, futures):
            result = ray.get(future)
            store_motif_cycles_bulk(result)

        # Re-query for output
        conn = connect_db("motif_cycles.db")
        cursor = conn.cursor()

        root = get_project_root()
        motif_params_path = os.path.join(root, "databases", "motif_parameters.db")
        motif_integers_path = os.path.join(root, "databases", "motif_integers.db")

        cursor.execute("ATTACH DATABASE ? AS mi", (motif_integers_path,))
        cursor.execute("ATTACH DATABASE ? AS params", (motif_params_path,))

        if TWIN_ONLY:
            filter_clause = "AND p.B = p.Y + 1"
        elif PARTITION_MODE:
            filter_clause = "AND p.B != p.Y + 1"
        else:
            filter_clause = ""

        cursor.execute(f"""
            SELECT c.motif_integer, c.cycle
            FROM motif_cycles c
            JOIN mi.motif_integers m ON c.motif_integer = m.integer_N
            JOIN params.motif_parameters p ON m.motif_A = p.A
            WHERE c.motif_integer <= ?
            {filter_clause}
        """, (INTEGER_LIMIT,))
        rows = cursor.fetchall()
        conn.close()

        for n, cycle_json in rows:
            file.write(f"{n}: {json.loads(cycle_json)}\n")

        if missing_motifs:
            file.write("\n### Missing Collatz Cycles (Not Found in collatz_sequences.db) ###\n")
            file.write(", ".join(map(str, missing_motifs)) + "\n")

    print(f"✅ Step 4 complete. New cycles saved to `motif_cycles.db` and written to:\n→ {output_file}")

if __name__ == "__main__":
    process_motif_cycles_ray()
