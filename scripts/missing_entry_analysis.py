#!/usr/bin/env python3
import sqlite3
import json
import os
import gc
import traceback
import ray

from config.config import TEXT_OUTPUT_PATH, DB_PATH, INTEGER_LIMIT, BATCH_SIZE
from config.db_utils import connect_db, setup_motif_entry_db

# -----------------------------
# Ray Initialization
# -----------------------------
ray.init(ignore_reinit_error=True, num_cpus=os.cpu_count() - 1)

# -----------------------------
# Ray Tasks
# -----------------------------
@ray.remote
def fetch_motif_integers():
    """Fetches all unique motif integers from full_lookup.db."""
    conn = connect_db("full_lookup.db")
    cursor = conn.cursor()
    motif_integers = set()

    cursor.execute("SELECT DISTINCT unique_int FROM full_lookup WHERE unique_int <= ?", (INTEGER_LIMIT,))
    while True:
        batch = cursor.fetchmany(BATCH_SIZE)
        if not batch:
            break
        motif_integers.update(row[0] for row in batch)

    conn.close()
    return motif_integers

@ray.remote
def process_batch(batch, motif_integers):
    """Processes one batch of Collatz sequences."""
    motif_entries = []
    missing_entries = []

    for n, sequence_json in batch:
        sequence = json.loads(sequence_json)
        if n not in motif_integers:
            entry_step, entered_motif = find_entry_point(sequence, motif_integers)
            if entry_step is not None:
                motif_entries.append((n, entry_step, entered_motif))
            else:
                missing_entries.append(n)

    return motif_entries, missing_entries

# -----------------------------
# Processing Logic
# -----------------------------
def find_entry_point(sequence, motif_integers):
    for step, value in enumerate(sequence):
        if value in motif_integers:
            return step, value
    return None, None

def fetch_sequences_in_batches():
    conn = connect_db("collatz_sequences.db")
    cursor = conn.cursor()
    cursor.execute("SELECT n, sequence FROM collatz_cache WHERE n <= ?", (INTEGER_LIMIT,))

    while True:
        batch = cursor.fetchmany(BATCH_SIZE)
        if not batch:
            break
        yield batch

    conn.close()

def store_motif_entries(motif_entries):
    conn = connect_db("motif_entry_points.db")
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT OR IGNORE INTO motif_entry_points (integer_N, entry_step, entered_motif)
        VALUES (?, ?, ?)
    ''', motif_entries)
    conn.commit()
    conn.close()

def save_text_output(filename, data, title):
    file_path = os.path.join(TEXT_OUTPUT_PATH, filename)
    with open(file_path, "w") as file:
        file.write(f"### {title} Up to {INTEGER_LIMIT} ###\n\n")
        if "missing" in filename:
            file.write("\n".join(map(str, data)))
        else:
            for n, entry_step, entered_motif in data:
                file.write(f"{n}: Entered a motif at integer {entered_motif}, at step {entry_step}\n")
    print(f"✅ {title} saved to `{file_path}`.")

# -----------------------------
# Master Routine
# -----------------------------
def process_missing_entries():
    setup_motif_entry_db()
    print("🔍 Processing missing entries using Ray...")

    motif_integers = ray.get(fetch_motif_integers.remote())
    tasks = [process_batch.remote(batch, motif_integers) for batch in fetch_sequences_in_batches()]
    results = ray.get(tasks)

    all_motif_entries = []
    all_missing_entries = []

    for motif_entries, missing_entries in results:
        all_motif_entries.extend(motif_entries)
        all_missing_entries.extend(missing_entries)

    store_motif_entries(all_motif_entries)
    save_text_output("missing_entries.txt", all_missing_entries, "Missing Entries")
    save_text_output("motif_entry_points.txt", all_motif_entries, "Motif Entry Points")
    print("✅ Processing complete.")

if __name__ == "__main__":
    try:
        process_missing_entries()
    except Exception as e:
        print("❌ Script crashed!")
        traceback.print_exc()
