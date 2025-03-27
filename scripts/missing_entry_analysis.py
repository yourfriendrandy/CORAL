#!/usr/bin/env python3
import sqlite3
import json
import os
import traceback
import ray

from config.config import (
    TEXT_OUTPUT_PATH, INTEGER_LIMIT, BATCH_SIZE,
    TWIN_ONLY, PARTITION_MODE, print_active_mode
)
from config.db_utils import connect_db, setup_motif_entry_db, get_project_root

# -----------------------------
# Ray Initialization
# -----------------------------
ray.shutdown()
ray.init(ignore_reinit_error=True, num_cpus=os.cpu_count() - 1)

# -----------------------------
# Mode-Based Table Selection
# -----------------------------
if TWIN_ONLY:
    LOOKUP_TABLE = "full_lookup_twin"
    ENTRY_TABLE = "motif_entry_points_twin"
elif PARTITION_MODE:
    LOOKUP_TABLE = "full_lookup_partitioned"
    ENTRY_TABLE = "motif_entry_points_partitioned"
else:
    LOOKUP_TABLE = "full_lookup"
    ENTRY_TABLE = "motif_entry_points"

# -----------------------------
# Ray Tasks
# -----------------------------
@ray.remote
def fetch_motif_integers():
    """Fetches all unique motif integers from full_lookup db."""
    db_path = os.path.join(get_project_root(), "databases", "full_lookup.db")
    conn = connect_db(db_path)
    cursor = conn.cursor()
    motif_integers = set()

    cursor.execute(f"SELECT DISTINCT unique_int FROM {LOOKUP_TABLE} WHERE unique_int <= ?", (INTEGER_LIMIT,))
    while True:
        batch = cursor.fetchmany(BATCH_SIZE)
        if not batch:
            break
        motif_integers.update(row[0] for row in batch)

    conn.close()
    return motif_integers

@ray.remote
def process_batch(batch, motif_integers):
    motif_entries = []            # For DB
    display_entries = []          # For motif_entry_points.txt
    missing_entries = []          # For missing_entries.txt
    exceptions_3n2x = []          # For three_n_times_2x_exceptions.txt (now with metadata)

    for n, sequence_json in batch:
        sequence = json.loads(sequence_json)

        is_form, base_n, X = is_three_n_times_power_of_two(n, return_components=True)
        entry_step, entered_motif = find_entry_point(sequence, motif_integers)

        if is_form:
            if entry_step is not None and entered_motif != 4:
                exceptions_3n2x.append((n, base_n, X, entry_step, entered_motif))
            else:
                exceptions_3n2x.append((n, base_n, X, None, None))

        if n not in motif_integers:
            if entry_step is not None and entered_motif != 4:
                motif_entries.append((n, entry_step, entered_motif))
                if not is_form:
                    display_entries.append((n, entry_step, entered_motif))
            else:
                missing_entries.append(n)

    return motif_entries, display_entries, missing_entries, exceptions_3n2x

# -----------------------------
# Helper Functions
# -----------------------------
def find_entry_point(sequence, motif_integers):
    for step, value in enumerate(sequence):
        if value in motif_integers:
            return step, value
    return None, None

def is_three_n_times_power_of_two(n, return_components=False):
    if n % 3 != 0:
        return (False, None, None) if return_components else False
    m = n // 3
    if m <= 0:
        return (False, None, None) if return_components else False

    X = 0
    while m % 2 == 0:
        m //= 2
        X += 1

    if m % 2 == 1:
        return (True, m, X) if return_components else True
    return (False, None, None) if return_components else False

def fetch_sequences_in_batches():
    db_path = os.path.join(get_project_root(), "databases", "collatz_sequences.db")
    conn = connect_db(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT n, sequence FROM collatz_cache WHERE n <= ?", (INTEGER_LIMIT,))
    while True:
        batch = cursor.fetchmany(BATCH_SIZE)
        if not batch:
            break
        yield batch
    conn.close()

def store_motif_entries(motif_entries):
    db_path = os.path.join(get_project_root(), "databases", "motif_entry_points.db")
    conn = connect_db(db_path)
    cursor = conn.cursor()
    cursor.executemany(f'''
        INSERT OR IGNORE INTO {ENTRY_TABLE} (integer_N, entry_step, entered_motif)
        VALUES (?, ?, ?)
    ''', motif_entries)
    conn.commit()
    conn.close()

def save_text_output(filename, data, title):
    file_path = os.path.join(TEXT_OUTPUT_PATH, filename)
    with open(file_path, "w") as file:
        file.write(f"### {title} Up to {INTEGER_LIMIT} ###\n\n")

        if not data:
            file.write("[No entries found]\n")
            print(f"⚠️  No data for `{title}` — empty file written to `{file_path}`.")
            return

        if filename == "three_n_times_2x_exceptions.txt":
            for n, base_n, X, entry_step, entered_motif in data:
                line = f"{n}: Form (3*{base_n}) * 2^{X}"
                if entry_step is not None and entered_motif is not None:
                    line += f" → enters motif at {entered_motif}, step {entry_step}"
                file.write(line + "\n")
        elif isinstance(data[0], tuple):
            for n, entry_step, entered_motif in data:
                file.write(f"{n}: Entered a motif at integer {entered_motif}, at step {entry_step}\n")
        else:
            file.write("\n".join(map(str, data)))

    print(f"✅ {title} saved to `{file_path}`.")

# -----------------------------
# Main Routine
# -----------------------------
def process_missing_entries():
    print_active_mode()
    setup_motif_entry_db()
    print("🔍 Analyzing missing entries with Ray...")

    motif_integers = ray.get(fetch_motif_integers.remote())
    print(f"✅ Pulled {len(motif_integers)} known motif integers from `{LOOKUP_TABLE}`")
    print(f"ℹ️  First few: {sorted(list(motif_integers))[:10]}")
    tasks = [process_batch.remote(batch, motif_integers) for batch in fetch_sequences_in_batches()]
    results = ray.get(tasks)

    all_motif_entries = []
    all_display_entries = []      # Excludes (3n)*(2^X)
    all_missing_entries = []
    all_exceptions = []

    for motif_entries, display_entries, missing_entries, exceptions in results:
        all_motif_entries.extend(motif_entries)
        all_display_entries.extend(display_entries)
        all_missing_entries.extend(missing_entries)
        all_exceptions.extend(exceptions)

    store_motif_entries(all_motif_entries)
    save_text_output("motif_entry_points.txt", all_display_entries, "Motif Entry Points (Excludes (3n)*2^X)")
    save_text_output("missing_entries.txt", all_missing_entries, "Missing Entries")
    save_text_output("three_n_times_2x_exceptions.txt", all_exceptions, "(3n) * (2^X) Exceptions")

    print("✅ Missing entry analysis complete.")

# -----------------------------
# Entry Point
# -----------------------------
if __name__ == "__main__":
    try:
        process_missing_entries()
    except Exception:
        print("❌ Script crashed!")
        traceback.print_exc()
